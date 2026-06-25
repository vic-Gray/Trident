// Package handlers contains the HTTP handler functions for the Trident REST API.
package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/Depo-dev/trident/services/api/grpcclient"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const healthStalenessThreshold = 60 * time.Second

// HealthRow holds the columns we read from system_state for the health check.
type HealthRow struct {
	LastLedgerIndexed sql.NullInt64
	LastPollAt        sql.NullTime
}

// HealthResponse is the JSON body returned by GET /v1/health.
type HealthResponse struct {
	Status  string `json:"status"`
	Indexer struct {
		LastLedgerIndexed *int64  `json:"last_ledger_indexed"`
		LastPollAt        *string `json:"last_poll_at"`
	} `json:"indexer"`
	GRPCApi string `json:"grpc_api,omitempty"`
}

// Health handles GET /v1/health.
//
// Acceptance criteria (issue #62 / #45):
//   - Returns the indexer's last_ledger_indexed and last_poll_at from system_state.
//   - Returns HTTP 503 with status "degraded" when last_poll_at is NULL or older than 60 s.
//   - Returns HTTP 200 with status "ok" otherwise.
//   - Reflects gRPC connectivity state in the "grpc_api" field when a conn is provided.
//
// db may be nil when DATABASE_URL is not configured; returns 503 in that case.
// grpcConn may be nil when the gRPC client is not yet wired in.
func Health(db *pgx.Conn, grpcConn *grpc.ClientConn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var resp HealthResponse

		// Reflect gRPC connection state (issue #45).
		if grpcConn != nil {
			state := grpcConn.GetState()
			resp.GRPCApi = grpcclient.StateString(state)
			if state == connectivity.TransientFailure || state == connectivity.Shutdown {
				resp.Status = "degraded"
			}
		}

		if db == nil {
			resp.Status = "degraded"
			writeJSON(w, http.StatusServiceUnavailable, resp)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var lastLedger *int64
		var lastPollAt *time.Time

		row := db.QueryRow(ctx,
			`SELECT last_ledger_indexed, last_poll_at
			   FROM system_state
			  WHERE key = 'latest_ledger_cursor'`,
		)

		// Scan into nullable pointers directly using pgx semantics.
		err := row.Scan(&lastLedger, &lastPollAt)
		if err != nil && err != pgx.ErrNoRows {
			resp.Status = "degraded"
			writeJSON(w, http.StatusServiceUnavailable, resp)
			return
		}

		if lastLedger != nil {
			resp.Indexer.LastLedgerIndexed = lastLedger
		}
		if lastPollAt != nil {
			s := lastPollAt.UTC().Format(time.RFC3339)
			resp.Indexer.LastPollAt = &s
		}

		// Degraded if last_poll_at is null or older than 60 seconds.
		if lastPollAt == nil || time.Since(*lastPollAt) > healthStalenessThreshold {
			resp.Status = "degraded"
			writeJSON(w, http.StatusServiceUnavailable, resp)
			return
		}

		if resp.Status == "" {
			resp.Status = "ok"
		}
		code := http.StatusOK
		if resp.Status != "ok" {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, resp)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
