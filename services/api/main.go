package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/Depo-dev/trident/services/api/ws"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// defaultDBPoolSize is the per-replica Postgres pool size for the Go API. With
// PgBouncer in front, the total against Postgres is this value times the replica
// count, which must stay within PgBouncer's default_pool_size (issue #87).
const defaultDBPoolSize = 5

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	// Open a bounded Postgres connection pool for the data-backed endpoints.
	// In production DATABASE_URL points at PgBouncer (transaction mode), so the
	// pool uses the simple query protocol to avoid server-side prepared
	// statements that do not survive across pooled transactions.
	// DATABASE_URL must be set; if absent, DB-backed endpoints return 503.
	var pool *pgxpool.Pool

	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		p, err := newDBPool(ctx, dsn, dbPoolSizeFromEnv())
		cancel()
		if err != nil {
			slog.Warn("could not connect to database; DB-backed endpoints will return 503", "err", err)
		} else {
			pool = p
			defer pool.Close()
		}
	} else {
		slog.Warn("DATABASE_URL not set; DB-backed endpoints will return 503")
	}

	// Keep the health DB as an untyped-nil interface when the pool is absent so
	// the handler's nil check fires instead of dereferencing a typed-nil pool.
	var healthDB handlers.DBPool
	if pool != nil {
		healthDB = pool
	}

	// ---------------------------------------------------------------------------
	// Redis client
	// ---------------------------------------------------------------------------
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		slog.Error("invalid REDIS_URL", "err", err)
		os.Exit(1)
	}

	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	// ---------------------------------------------------------------------------
	// WebSocket hub + Redis Streams consumer
	// ---------------------------------------------------------------------------
	hub := ws.NewHub()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	go ws.StartConsumer(ctx, redisClient, hub)

	// ---------------------------------------------------------------------------
	// HTTP router
	// ---------------------------------------------------------------------------
	mux := http.NewServeMux()

	// GET /v1/health — indexer liveness (issue #62)
	mux.HandleFunc("GET /v1/health", handlers.Health(healthDB))

	// GET /v1/events — validated, cursor-paginated event listing (issues #42, #44)
	mux.HandleFunc("GET /v1/events", handlers.ListEvents)

	// GET /v1/events/{id} — single event by UUID v4 (issue #42)
	mux.HandleFunc("GET /v1/events/{id}", handlers.GetEvent)

	// GET /v1/admin/db — PgBouncer pool stats for capacity planning (issue #87)
	mux.HandleFunc("GET /v1/admin/db", handlers.AdminDB(adminConfig()))

	// WebSocket: /ws — real-time event subscription endpoint (issue #15)
	mux.HandleFunc("/ws", ws.Handler(hub))

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		slog.Info("Trident API server listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "err", err)
	}
}

// newDBPool builds a bounded pgx pool. It forces the simple query protocol so
// the pool is safe behind PgBouncer in transaction mode, where server-side
// prepared statements are not preserved across pooled transactions (issue #87).
func newDBPool(ctx context.Context, dsn string, poolSize int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}

	cfg.MaxConns = poolSize
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}

// dbPoolSizeFromEnv reads GO_API_DB_POOL_SIZE, falling back to the default for a
// missing, non-numeric, or non-positive value.
func dbPoolSizeFromEnv() int32 {
	if raw := os.Getenv("GO_API_DB_POOL_SIZE"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return int32(n)
		}

		slog.Warn(
			"invalid GO_API_DB_POOL_SIZE; using default",
			"value", raw,
			"default", defaultDBPoolSize,
		)
	}

	return defaultDBPoolSize
}

// adminConfig assembles the admin DB endpoint configuration from the
// environment. When ADMIN_API_KEY or PGBOUNCER_ADMIN_URL is unset the endpoint
// stays disabled (returns 503).
func adminConfig() handlers.AdminConfig {
	cfg := handlers.AdminConfig{
		AdminKey: os.Getenv("ADMIN_API_KEY"),
	}

	if adminURL := os.Getenv("PGBOUNCER_ADMIN_URL"); adminURL != "" {
		cfg.StatsFunc = newPgbouncerStats(adminURL)
	}

	return cfg
}