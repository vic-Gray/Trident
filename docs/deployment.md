# Deployment

This document covers running Trident in production, with a focus on the database
connection topology. For local development see `docker/docker-compose.dev.yml`
and the project README.

## Connection topology

Trident runs three database clients:

| Service              | Role                         | Pool env var            | Default |
| -------------------- | ---------------------------- | ----------------------- | ------- |
| Indexer (Rust)       | single writer, low write QPS | `INDEXER_DB_POOL_SIZE`  | 3       |
| gRPC API (Rust)      | read-heavy, moderate QPS     | `GRPC_API_DB_POOL_SIZE` | 10      |
| REST API (Go)        | per-replica request handler  | `GO_API_DB_POOL_SIZE`   | 5       |

In production every service connects to **PgBouncer**, never to Postgres
directly. PgBouncer (transaction pooling mode) multiplexes the many short-lived
application connections over a small set of real Postgres connections, so
Postgres never approaches its `max_connections` limit even as the Go API scales
to multiple replicas.

```
indexer  ─┐
gRPC API ─┼─▶  PgBouncer (pgbouncer:6432, transaction mode)  ─▶  Postgres :5432
Go API   ─┘        default_pool_size = 20
(N replicas)
```

The production compose (`docker/docker-compose.yml`) wires this up: each
service's `DATABASE_URL` is rewritten to
`postgres://<user>:<pass>@pgbouncer:6432/<db>` and PgBouncer is configured to
talk to `postgres:5432`. No service config references `postgres:5432` directly.

### Sizing the pools

Each `*_DB_POOL_SIZE` is **per instance**. The total number of connections a
tier opens against PgBouncer is:

```
total = pool_size * number_of_replicas
```

PgBouncer's `PGBOUNCER_DEFAULT_POOL_SIZE` is the ceiling on the real Postgres
connections it keeps open per `(user, database)` pair, and it must be **greater
than or equal to** the sum of the active demand across every tier. With the
defaults above and three Go API replicas:

```
indexer:  3
gRPC API: 10
Go API:   5 * 3 replicas = 15
-------------------------------
peak demand: 28 logical connections, multiplexed by PgBouncer
```

Because most of those connections are idle between transactions, a
`default_pool_size` of 20 comfortably serves this load while keeping Postgres
well under `max_connections=100`. Raise `default_pool_size` and Postgres
`max_connections` together when scaling out further.

## PgBouncer transaction mode: common pitfalls

Transaction pooling is what makes PgBouncer efficient (a server connection is
held only for the duration of a transaction), but it means **no session state
may be assumed to survive across transaction boundaries**. The next transaction
from the same client may be served by a different physical Postgres connection.

Concretely, the following do **not** work in transaction mode:

1. **Named/server-side prepared statements.** A prepared statement lives on one
   server connection; the next transaction may land on another connection where
   it does not exist, and the query fails. This is the most common failure when
   adopting PgBouncer.
2. **`SET SESSION` variables.** They are not preserved across transactions.
3. **Session-level advisory locks.** They behave unexpectedly because the
   "session" is not stable.

Trident's clients are configured to avoid (1):

- **Rust (sqlx):** the pool disables the prepared-statement cache via
  `PgConnectOptions::statement_cache_capacity(0)`. See `connect_pool` in
  `crates/indexer/src/db/mod.rs` and the equivalent in `crates/api/src/main.rs`.
- **Go (pgx v5):** the pool forces the simple query protocol with
  `cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol`. (In
  pgx v4 this was `PreferSimpleProtocol: true`.) See `newDBPool` in
  `services/api/main.go`.

### Schema migrations

Run migrations against a **direct** Postgres connection (or a session-mode
pooler), not the transaction-mode pooler — migrations frequently rely on
session-scoped statements and locks. Keep a direct DSN available for that
purpose; do not point your migration tool at `pgbouncer:6432`.

## Admin stats endpoint

The Go API exposes `GET /v1/admin/db` for capacity planning. It proxies
PgBouncer's admin console (`SHOW POOLS` and `SHOW STATS`) and returns the rows
as JSON, so you can watch pool utilisation, client wait times, and connection
counts.

- Set `ADMIN_API_KEY` to a random secret (`openssl rand -hex 32`). The endpoint
  is disabled (returns `503`) when the key is unset.
- Set `PGBOUNCER_ADMIN_URL` to the PgBouncer admin console DSN — connect to the
  virtual `pgbouncer` database, e.g.
  `postgres://<user>:<pass>@pgbouncer:6432/pgbouncer`. The user must be listed in
  PgBouncer's `admin_users`/`stats_users` (the compose sets this to the Postgres
  user).
- Call it with the secret in the `X-Admin-Key` header:

```bash
curl -H "X-Admin-Key: $ADMIN_API_KEY" http://localhost:3000/v1/admin/db
```

A missing or wrong key returns `401`; an unreachable PgBouncer returns `502`.

## Fly.io

On Fly.io use **Fly Managed Postgres**, which ships with a built-in connection
pooler (PgBouncer-equivalent). You do not deploy the `pgbouncer` compose service
there.

- Use the **pooler** connection string — the Fly Postgres proxy on **port
  5432** — as your `DATABASE_URL`. This is the pooled, transaction-mode
  endpoint.
- Do **not** use the **direct** port **5433**; that bypasses the pooler and
  reintroduces the connection-exhaustion problem this setup exists to prevent.
- The same transaction-mode caveats above apply, so keep the simple-protocol /
  no-statement-cache configuration in place. Use the direct port (5433) only for
  one-off migrations.

## Load testing

`load-tests/pgbouncer-validation.js` is a [k6](https://k6.io) script that drives
100 concurrent clients, each issuing 10 requests to `GET /v1/events`, and asserts
no `too many connections` errors with p99 latency under 500ms. Run it against the
stack both with and without PgBouncer to confirm the improvement:

```bash
BASE_URL=http://localhost:3000 k6 run load-tests/pgbouncer-validation.js
```
