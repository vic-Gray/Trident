package ws

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

const (
	// streamKey is the Redis Stream written by the Rust indexer.
	streamKey = "trident:events"
)

// streamEntry is the JSON structure expected in each Redis Stream message value.
type streamEntry struct {
	ContractID string          `json:"contract_id"`
	Payload    json.RawMessage `json:"payload"`
}

// StartConsumer begins a blocking XREAD loop against the Redis Stream at
// trident:events, deserialises each entry, and fans the raw JSON payload
// out to connected WebSocket clients via hub.Broadcast.
//
// It runs until ctx is cancelled, at which point it returns cleanly.
// Intended to be launched as a goroutine: go StartConsumer(ctx, redisClient, hub).
func StartConsumer(ctx context.Context, rdb *redis.Client, hub *Hub) {
	lastID := "$" // only deliver messages that arrive after startup

	slog.Info("ws: Redis Streams consumer starting", "stream", streamKey)

	for {
		// Block until at least one new message arrives or the context is cancelled.
		entries, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamKey, lastID},
			Block:   0, // block indefinitely
			Count:   100,
		}).Result()

		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled — clean shutdown.
				slog.Info("ws: Redis Streams consumer stopped")
				return
			}
			slog.Error("ws: XRead error", "err", err)
			// Back off briefly on transient errors to avoid a tight error loop.
			select {
			case <-ctx.Done():
				return
			default:
			}
			continue
		}

		for _, stream := range entries {
			for _, msg := range stream.Messages {
				lastID = msg.ID

				raw, ok := msg.Values["data"]
				if !ok {
					slog.Warn("ws: stream message missing 'data' field", "id", msg.ID)
					continue
				}

				rawStr, ok := raw.(string)
				if !ok {
					slog.Warn("ws: stream message 'data' is not a string", "id", msg.ID)
					continue
				}

				var entry streamEntry
				if err := json.Unmarshal([]byte(rawStr), &entry); err != nil {
					slog.Warn("ws: failed to unmarshal stream entry", "id", msg.ID, "err", err)
					continue
				}

				if entry.ContractID == "" {
					slog.Warn("ws: stream entry missing contract_id", "id", msg.ID)
					continue
				}

				hub.Broadcast(entry.ContractID, entry.Payload)
			}
		}
	}
}
