// Package ws implements a WebSocket hub that fans out contract events from
// Redis Streams to connected browser/client subscribers.
package ws

import (
	"log/slog"
	"sync"
)

// client represents a single connected WebSocket subscriber.
type client struct {
	contractID string
	send       chan []byte
}

// Hub manages all active WebSocket clients and routes broadcast messages to
// the correct subscribers based on contractId.
type Hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
}

// NewHub constructs a Hub ready to use. Call Run in a goroutine before
// accepting any WebSocket connections.
func NewHub() *Hub {
	return &Hub{
		clients: make(map[*client]struct{}),
	}
}

// register adds c to the hub's active set. Safe to call concurrently.
func (h *Hub) register(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	slog.Debug("ws: client registered", "contractId", c.contractID)
}

// unregister removes c from the hub and closes its send channel so the
// goroutine draining it can exit cleanly.
func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
	h.mu.Unlock()
	slog.Debug("ws: client unregistered", "contractId", c.contractID)
}

// Broadcast delivers msg to every client subscribed to contractID.
// It is safe to call from any goroutine (e.g. the Redis consumer).
// Clients whose send buffer is full are dropped to avoid blocking the caller.
func (h *Hub) Broadcast(contractID string, msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for c := range h.clients {
		if c.contractID != contractID {
			continue
		}
		select {
		case c.send <- msg:
		default:
			slog.Warn("ws: dropping message for slow client", "contractId", contractID)
		}
	}
}
