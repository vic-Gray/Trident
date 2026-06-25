package ws_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/ws"
)

// recvTimeout bounds how long a test waits for an expected frame. It only fires
// on failure: because Broadcast is synchronous, a matching frame is always
// already buffered by the time the test reads, so the happy path never sleeps.
const recvTimeout = time.Second

// frame mirrors the JSON the hub writes to clients (see wireEvent in hub.go).
type frame struct {
	ContractID      string `json:"contract_id"`
	LedgerSequence  string `json:"ledger_sequence"`
	LedgerTimestamp string `json:"ledger_timestamp"`
	TransactionHash string `json:"transaction_hash"`
	EventIndex      string `json:"event_index"`
	EventType       string `json:"event_type"`
	Topics          string `json:"topics"`
	Data            string `json:"data"`
}

// recv reads exactly one frame from c, failing the test if none arrives or the
// channel is closed.
func recv(t *testing.T, c *ws.Client) frame {
	t.Helper()
	select {
	case msg, ok := <-c.Send():
		if !ok {
			t.Fatal("client send channel closed unexpectedly")
		}
		var f frame
		if err := json.Unmarshal(msg, &f); err != nil {
			t.Fatalf("unmarshal frame: %v", err)
		}
		return f
	case <-time.After(recvTimeout):
		t.Fatal("timed out waiting for an event frame")
		return frame{}
	}
}

// mockReader is an in-memory Reader: queued events are returned in order, and
// once the queue drains Read blocks until ctx is cancelled. It lets the hub run
// without a real Redis connection.
type mockReader struct {
	events chan ws.Event
}

func newMockReader(buffer int) *mockReader {
	return &mockReader{events: make(chan ws.Event, buffer)}
}

func (m *mockReader) Read(ctx context.Context) (ws.Event, error) {
	select {
	case ev := <-m.events:
		return ev, nil
	case <-ctx.Done():
		return ws.Event{}, ctx.Err()
	}
}

// Fan-out correctness: every client on a contract gets exactly one matching
// message, and the payload round-trips.
func TestHub_Broadcast_FansOutToAllSubscribers(t *testing.T) {
	const contract = "CCONTRACTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	hub := ws.NewHub(nil)

	clients := []*ws.Client{
		ws.NewClient(contract, ""),
		ws.NewClient(contract, ""),
		ws.NewClient(contract, ""),
	}
	for _, c := range clients {
		hub.Register(c)
	}

	ev := ws.Event{
		ContractID:      contract,
		LedgerSequence:  42,
		LedgerTimestamp: "2026-06-25T00:00:00Z",
		TransactionHash: "deadbeef",
		EventIndex:      1,
		EventType:       "contract",
		Topics:          []string{"transfer"},
		Data:            `{"amount":"100"}`,
	}
	hub.Broadcast(ev)

	for i, c := range clients {
		f := recv(t, c)
		if f.ContractID != ev.ContractID {
			t.Errorf("client %d: contract_id = %q, want %q", i, f.ContractID, ev.ContractID)
		}
		if f.LedgerSequence != "42" {
			t.Errorf("client %d: ledger_sequence = %q, want %q", i, f.LedgerSequence, "42")
		}
		if f.EventIndex != "1" {
			t.Errorf("client %d: event_index = %q, want %q", i, f.EventIndex, "1")
		}
		if f.EventType != ev.EventType {
			t.Errorf("client %d: event_type = %q, want %q", i, f.EventType, ev.EventType)
		}
		if f.Topics != `["transfer"]` {
			t.Errorf("client %d: topics = %q, want %q", i, f.Topics, `["transfer"]`)
		}
		if f.Data != ev.Data {
			t.Errorf("client %d: data = %q, want %q", i, f.Data, ev.Data)
		}
		if n := len(c.Send()); n != 0 {
			t.Errorf("client %d: %d extra messages buffered, want exactly one delivered", i, n)
		}
	}
}

// Contract filter isolation: a message for contract C1 must never reach a
// client subscribed to C2.
func TestHub_Broadcast_IsolatesByContract(t *testing.T) {
	const (
		c1 = "CCONTRACT1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		c2 = "CCONTRACT2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	)
	hub := ws.NewHub(nil)
	a := ws.NewClient(c1, "")
	b := ws.NewClient(c2, "")
	hub.Register(a)
	hub.Register(b)

	hub.Broadcast(ws.Event{ContractID: c1, Topics: []string{"transfer"}})

	if f := recv(t, a); f.ContractID != c1 {
		t.Errorf("client A: contract_id = %q, want %q", f.ContractID, c1)
	}
	if n := len(b.Send()); n != 0 {
		t.Errorf("client B received %d messages, want 0", n)
	}
}

// topic_0 filter: a client with topic0="transfer" sees only transfers, while a
// client with no topic filter sees every event for the contract.
func TestHub_Broadcast_AppliesTopic0Filter(t *testing.T) {
	const contract = "CCONTRACT1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	hub := ws.NewHub(nil)
	filtered := ws.NewClient(contract, "transfer") // transfers only
	all := ws.NewClient(contract, "")              // everything

	hub.Register(filtered)
	hub.Register(all)

	hub.Broadcast(ws.Event{ContractID: contract, EventType: "contract", Topics: []string{"transfer"}})
	hub.Broadcast(ws.Event{ContractID: contract, EventType: "contract", Topics: []string{"mint"}})

	// Filtered client receives only the transfer.
	if f := recv(t, filtered); f.Topics != `["transfer"]` {
		t.Errorf("filtered client topics = %q, want %q", f.Topics, `["transfer"]`)
	}
	if n := len(filtered.Send()); n != 0 {
		t.Errorf("filtered client received %d extra messages, want 0", n)
	}

	// Unfiltered client receives both, in order.
	if f := recv(t, all); f.Topics != `["transfer"]` {
		t.Errorf("unfiltered client first frame topics = %q, want %q", f.Topics, `["transfer"]`)
	}
	if f := recv(t, all); f.Topics != `["mint"]` {
		t.Errorf("unfiltered client second frame topics = %q, want %q", f.Topics, `["mint"]`)
	}
}

// Disconnect cleanup: an unregistered client is removed from the hub, its send
// channel is closed, and a subsequent broadcast neither reaches it nor panics
// on the closed channel. Unregister is also idempotent.
func TestHub_Unregister_RemovesClientAndIsBroadcastSafe(t *testing.T) {
	const contract = "CCONTRACT1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	hub := ws.NewHub(nil)
	c := ws.NewClient(contract, "")

	hub.Register(c)
	if got := hub.ClientCount(); got != 1 {
		t.Fatalf("ClientCount after register = %d, want 1", got)
	}

	hub.Unregister(c)
	if got := hub.ClientCount(); got != 0 {
		t.Fatalf("ClientCount after unregister = %d, want 0", got)
	}

	// The send channel must be closed.
	if _, ok := <-c.Send(); ok {
		t.Error("client send channel should be closed after unregister")
	}

	// Broadcasting for the disconnected client's contract must not attempt a
	// send on the closed channel (would panic) and must reach no one.
	hub.Broadcast(ws.Event{ContractID: contract, Topics: []string{"transfer"}})

	// A second unregister must be a no-op, not a double close.
	hub.Unregister(c)
}

// Concurrent register/unregister under the race detector: 50 goroutines each
// register and immediately unregister a client while the hub's broadcast loop
// runs concurrently off a mock reader. Run with `go test -race`.
func TestHub_ConcurrentRegisterUnregister(t *testing.T) {
	const contract = "CCONTRACT1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	reader := newMockReader(0)
	hub := ws.NewHub(reader)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the broadcast loop concurrently.
	runDone := make(chan struct{})
	go func() {
		_ = hub.Run(ctx)
		close(runDone)
	}()

	// Continuously feed events while clients churn, stopping on cancel.
	go func() {
		ev := ws.Event{ContractID: contract, Topics: []string{"transfer"}}
		for {
			select {
			case reader.events <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := ws.NewClient(contract, "")
			hub.Register(c)
			hub.Unregister(c)
		}()
	}
	wg.Wait()

	cancel()
	<-runDone

	if got := hub.ClientCount(); got != 0 {
		t.Errorf("ClientCount after concurrent churn = %d, want 0", got)
	}
}
