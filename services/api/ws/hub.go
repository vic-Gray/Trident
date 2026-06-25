// Package ws implements Trident's real-time WebSocket fan-out hub.
//
// The hub is the heart of live event delivery: it sources decoded contract
// events from the Redis Stream (through an injected Reader) and pushes each one
// to every connected client whose subscription matches.
//
// The hub is deliberately transport-agnostic. A Client only needs a buffered
// send channel, so the register / unregister / broadcast and filtering logic
// can be exercised without a real network connection, WebSocket handshake, or
// Redis instance — see hub_test.go.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
)

// defaultSendBuffer is the capacity of a client's outbound channel. A client
// that fills its buffer is considered too slow and is dropped, so a single
// stalled consumer can never block the fan-out for everyone else.
const defaultSendBuffer = 16

// Event is a decoded Soroban contract event ready for fan-out. It mirrors the
// fields of the proto Event relevant to live streaming (proto/trident.proto).
type Event struct {
	ContractID      string
	LedgerSequence  uint64
	LedgerTimestamp string
	TransactionHash string
	EventIndex      uint32
	EventType       string
	Topics          []string
	Data            string
}

// wireEvent is the JSON frame delivered to clients. Numeric fields are encoded
// as strings and Topics as a JSON-encoded array string to match the shape the
// TypeScript SDK decodes (see sdk/typescript/src/subscription.ts).
type wireEvent struct {
	ContractID      string `json:"contract_id"`
	LedgerSequence  string `json:"ledger_sequence"`
	LedgerTimestamp string `json:"ledger_timestamp"`
	TransactionHash string `json:"transaction_hash"`
	EventIndex      string `json:"event_index"`
	EventType       string `json:"event_type"`
	Topics          string `json:"topics"`
	Data            string `json:"data"`
}

// marshalEvent renders ev as the JSON frame sent over the wire.
func marshalEvent(ev Event) ([]byte, error) {
	topics := ev.Topics
	if topics == nil {
		topics = []string{}
	}
	topicsJSON, err := json.Marshal(topics)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wireEvent{
		ContractID:      ev.ContractID,
		LedgerSequence:  strconv.FormatUint(ev.LedgerSequence, 10),
		LedgerTimestamp: ev.LedgerTimestamp,
		TransactionHash: ev.TransactionHash,
		EventIndex:      strconv.FormatUint(uint64(ev.EventIndex), 10),
		EventType:       ev.EventType,
		Topics:          string(topicsJSON),
		Data:            ev.Data,
	})
}

// Reader is the minimal interface the hub needs to source events. The
// production implementation reads from the Redis Stream written by the Rust
// indexer; tests inject a mock so the hub can run without Redis.
type Reader interface {
	// Read blocks until the next event is available, ctx is cancelled, or an
	// unrecoverable error occurs. It must return a non-nil error when ctx is
	// done.
	Read(ctx context.Context) (Event, error)
}

// Client is a single subscriber. It is registered with a Hub and receives
// marshalled event frames on its send channel. A client subscribes to exactly
// one contract and may optionally filter on the first topic (topic_0).
type Client struct {
	// ContractID is the contract the client subscribed to. Required.
	ContractID string
	// Topic0, when non-empty, restricts delivery to events whose first topic
	// equals this value.
	Topic0 string

	send chan []byte
}

// NewClient creates a Client subscribed to contractID with an optional topic_0
// filter (pass "" for no topic filter).
func NewClient(contractID, topic0 string) *Client {
	return &Client{
		ContractID: contractID,
		Topic0:     topic0,
		send:       make(chan []byte, defaultSendBuffer),
	}
}

// Send returns the receive end of the client's outbound channel. A real
// connection's write pump ranges over this channel; the hub closes it when the
// client is unregistered.
func (c *Client) Send() <-chan []byte {
	return c.send
}

// matches reports whether ev should be delivered to this client.
func (c *Client) matches(ev Event) bool {
	if c.ContractID != ev.ContractID {
		return false
	}
	if c.Topic0 != "" && (len(ev.Topics) == 0 || ev.Topics[0] != c.Topic0) {
		return false
	}
	return true
}

// Hub maintains the set of connected clients and fans out events to them.
// All exported methods are safe for concurrent use.
type Hub struct {
	reader Reader

	mu      sync.RWMutex
	clients map[*Client]struct{}
}

// NewHub creates a Hub that sources events from reader. reader may be nil when
// the hub is driven directly via Broadcast (for example, in tests).
func NewHub(reader Reader) *Hub {
	return &Hub{
		reader:  reader,
		clients: make(map[*Client]struct{}),
	}
}

// Register adds c to the hub so it begins receiving matching events.
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

// Unregister removes c from the hub and closes its send channel. It is
// idempotent and safe to call for a client that was never registered or has
// already been dropped, so it never double-closes the channel.
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
}

// Broadcast delivers ev to every registered client whose subscription matches.
// A client whose send buffer is full is treated as dead: it is dropped from the
// hub and its channel closed, so a slow consumer can never stall the fan-out.
func (h *Hub) Broadcast(ev Event) {
	msg, err := marshalEvent(ev)
	if err != nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if !c.matches(ev) {
			continue
		}
		select {
		case c.send <- msg:
		default:
			delete(h.clients, c)
			close(c.send)
		}
	}
}

// ClientCount returns the number of currently registered clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Run drives the hub from its Reader, broadcasting each event until ctx is
// cancelled or the reader returns an error. It returns the error that caused it
// to stop (typically context.Canceled on a clean shutdown).
func (h *Hub) Run(ctx context.Context) error {
	if h.reader == nil {
		return errors.New("ws: hub has no reader")
	}
	for {
		ev, err := h.reader.Read(ctx)
		if err != nil {
			return err
		}
		h.Broadcast(ev)
	}
}
