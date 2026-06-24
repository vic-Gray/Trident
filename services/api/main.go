package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Depo-dev/trident/services/api/cursor"
	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/Depo-dev/trident/services/api/validation"
	"github.com/Depo-dev/trident/services/api/ws"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	// ---------------------------------------------------------------------------
	// Postgres connection (health endpoint)
	// ---------------------------------------------------------------------------
	var dbConn *pgx.Conn
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err != nil {
			slog.Warn("could not connect to database; health endpoint will return 503", "err", err)
		} else {
			dbConn = conn
			defer conn.Close(context.Background())
		}
	} else {
		slog.Warn("DATABASE_URL not set; health endpoint will return 503")
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

	// ---------------------------------------------------------------------------
	// WebSocket hub + Redis Streams consumer
	// ---------------------------------------------------------------------------
	hub := ws.NewHub()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go ws.StartConsumer(ctx, redisClient, hub)

	// ---------------------------------------------------------------------------
	// HTTP router
	// ---------------------------------------------------------------------------
	mux := http.NewServeMux()

	// GET /v1/health — indexer liveness (issue #62)
	mux.HandleFunc("GET /v1/health", handlers.Health(dbConn))

	// GET /v1/events — validated, cursor-paginated event listing (issues #42, #44)
	mux.HandleFunc("GET /v1/events", handleListEvents)

	// GET /v1/events/{id} — single event by UUID v4 (issue #42)
	mux.HandleFunc("GET /v1/events/{id}", handlers.GetEvent)

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

// handleListEvents handles GET /v1/events with query-param validation and
// opaque cursor-based pagination.
func handleListEvents(w http.ResponseWriter, r *http.Request) {
	type response struct {
		NextCursor string `json:"next_cursor"`
		Events     []any  `json:"events"`
	}

	q := r.URL.Query()

	// Validate query params (issue #42).
	_, verr := validation.ValidateQueryEvents(
		q.Get("limit"),
		q.Get("ledgerFrom"),
		q.Get("ledgerTo"),
		q.Get("contractId"),
		q.Get("cursor"),
	)
	if verr != nil {
		http.Error(w, verr.Error(), http.StatusBadRequest)
		return
	}

	// Decode opaque cursor to internal paging token (issue #44).
	var pagingToken string
	if raw := q.Get("cursor"); raw != "" {
		decoded, err := cursor.Decode(raw)
		if err != nil {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		pagingToken = decoded
	}

	nextCursor := cursor.Encode(pagingToken)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response{
		NextCursor: nextCursor,
		Events:     []any{},
	}); err != nil {
		slog.Error("handleListEvents: encode response", "err", err)
	}
}
