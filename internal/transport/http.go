package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

var (
	ErrClosed    = errors.New("transport is closed")
	ErrQueueFull = errors.New("transport queue is full")
	ErrPeer      = errors.New("unknown transport peer")
)

type Event struct {
	Type string
	Data map[string]any
}

type EventHandler func(Event)

const (
	EventQueued       = "transport.message.queued"
	EventSent         = "transport.message.sent"
	EventDelivered    = "transport.message.delivered"
	EventRetry        = "transport.retry"
	EventBackpressure = "transport.backpressure"
	EventFailure      = "transport.failure"
)

type outgoing struct {
	message  raft.Message
	address  string
	queuedAt time.Time
}

type peerWorker struct {
	id      raft.NodeID
	address string
	queue   chan outgoing

	mu sync.RWMutex

	enqueued      uint64
	sent          uint64
	delivered     uint64
	retries       uint64
	failures      uint64
	backpressure  uint64
	totalLatency  time.Duration
	lastLatency   time.Duration
	lastSuccessAt time.Time
	lastFailureAt time.Time
	lastError     string
}

type HTTPTransport struct {
	mu sync.Mutex

	peers  map[raft.NodeID]*peerWorker
	client *http.Client
	cancel context.CancelFunc
	done   chan struct{}
	errors chan error
	closed bool

	eventHandler EventHandler
}

func NewHTTPTransport(
	peers []Peer,
	queueSize int,
) (*HTTPTransport, error) {
	if queueSize <= 0 {
		return nil, errors.New("queue size must be positive")
	}

	ctx, cancel := context.WithCancel(context.Background())

	t := &HTTPTransport{
		peers:  make(map[raft.NodeID]*peerWorker),
		client: &http.Client{Timeout: 2 * time.Second},
		cancel: cancel,
		done:   make(chan struct{}),
		errors: make(chan error, queueSize),
	}

	for _, peer := range peers {
		if peer.ID == 0 || peer.Address == "" {
			cancel()
			return nil, errors.New("invalid transport peer")
		}

		if _, exists := t.peers[peer.ID]; exists {
			cancel()
			return nil, fmt.Errorf(
				"duplicate peer ID %d",
				peer.ID,
			)
		}

		t.peers[peer.ID] = &peerWorker{
			id:      peer.ID,
			address: peer.Address,
			queue:   make(chan outgoing, queueSize),
		}
	}

	var wg sync.WaitGroup

	for _, worker := range t.peers {
		wg.Add(1)

		go func(w *peerWorker) {
			defer wg.Done()
			t.runPeer(ctx, w)
		}(worker)
	}

	go func() {
		wg.Wait()
		close(t.done)
	}()

	return t, nil
}

func (t *HTTPTransport) SetEventHandler(
	handler EventHandler,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.eventHandler = handler
}

func (t *HTTPTransport) emit(
	eventType string,
	data map[string]any,
) {
	t.mu.Lock()
	handler := t.eventHandler
	t.mu.Unlock()

	if handler == nil {
		return
	}

	copied := make(map[string]any, len(data))

	for key, value := range data {
		copied[key] = value
	}

	handler(Event{
		Type: eventType,
		Data: copied,
	})
}

// Send validates destinations before enqueueing messages.
//
// Each peer has an independent queue. If one peer's queue is
// full, messages for other peers can still be accepted.
//
// ErrQueueFull indicates partial or complete rejection due
// to backpressure. Accepted messages remain ordered per peer.
func (t *HTTPTransport) Send(messages []raft.Message) error {
	t.mu.Lock()

	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}

	for _, message := range messages {
		if _, ok := t.peers[message.To]; !ok {
			t.mu.Unlock()

			return fmt.Errorf(
				"%w: %d",
				ErrPeer,
				message.To,
			)
		}
	}

	var fullPeers []raft.NodeID
	seenFull := make(map[raft.NodeID]bool)

	type emittedEvent struct {
		eventType string
		data      map[string]any
	}

	var emitted []emittedEvent

	for _, message := range messages {
		worker := t.peers[message.To]

		if len(worker.queue) == cap(worker.queue) {
			worker.mu.Lock()
			worker.backpressure++
			worker.mu.Unlock()

			if !seenFull[message.To] {
				seenFull[message.To] = true
				fullPeers = append(
					fullPeers,
					message.To,
				)
			}

			emitted = append(
				emitted,
				emittedEvent{
					eventType: EventBackpressure,
					data: map[string]any{
						"peer_id":        message.To,
						"queue_depth":    len(worker.queue),
						"queue_capacity": cap(worker.queue),
						"message_type":   message.Type,
						"request_id":     message.RequestID,
					},
				},
			)

			continue
		}

		now := time.Now()

		worker.queue <- outgoing{
			message:  message,
			address:  worker.address,
			queuedAt: now,
		}

		worker.mu.Lock()
		worker.enqueued++
		worker.mu.Unlock()

		emitted = append(
			emitted,
			emittedEvent{
				eventType: EventQueued,
				data: map[string]any{
					"peer_id":        message.To,
					"queue_depth":    len(worker.queue),
					"queue_capacity": cap(worker.queue),
					"message_type":   message.Type,
					"request_id":     message.RequestID,
				},
			},
		)
	}

	t.mu.Unlock()

	for _, event := range emitted {
		t.emit(event.eventType, event.data)
	}

	if len(fullPeers) > 0 {
		return fmt.Errorf(
			"%w: peers %v",
			ErrQueueFull,
			fullPeers,
		)
	}

	return nil
}

func (t *HTTPTransport) runPeer(
	ctx context.Context,
	worker *peerWorker,
) {
	for {
		select {
		case <-ctx.Done():
			return

		case item := <-worker.queue:
			if err := t.deliverWithRetry(
				ctx,
				item,
			); err != nil {
				select {
				case t.errors <- err:
				default:
				}
			}
		}
	}
}

func (t *HTTPTransport) deliver(
	ctx context.Context,
	worker *peerWorker,
	item outgoing,
	attempt int,
) error {
	payload, err := json.Marshal(item.message)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://"+item.address+"/raft/message",
		bytes.NewReader(payload),
	)
	if err != nil {
		return err
	}

	req.Header.Set(
		"Content-Type",
		"application/json",
	)

	started := time.Now()

	worker.mu.Lock()
	worker.sent++
	worker.mu.Unlock()

	t.emit(EventSent, map[string]any{
		"peer_id":      worker.id,
		"address":      worker.address,
		"message_type": item.message.Type,
		"request_id":   item.message.RequestID,
		"attempt":      attempt,
		"queue_delay_ms": float64(
			started.Sub(item.queuedAt).Microseconds(),
		) / 1000,
	})

	resp, err := t.client.Do(req)

	latency := time.Since(started)

	if err != nil {
		worker.mu.Lock()
		worker.lastFailureAt = time.Now()
		worker.lastError = err.Error()
		worker.mu.Unlock()

		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		err := fmt.Errorf(
			"peer returned HTTP %d",
			resp.StatusCode,
		)

		worker.mu.Lock()
		worker.lastFailureAt = time.Now()
		worker.lastError = err.Error()
		worker.mu.Unlock()

		return err
	}

	worker.mu.Lock()
	worker.delivered++
	worker.totalLatency += latency
	worker.lastLatency = latency
	worker.lastSuccessAt = time.Now()
	worker.lastError = ""
	worker.mu.Unlock()

	t.emit(EventDelivered, map[string]any{
		"peer_id":      worker.id,
		"address":      worker.address,
		"message_type": item.message.Type,
		"request_id":   item.message.RequestID,
		"attempt":      attempt,
		"latency_ms": float64(
			latency.Microseconds(),
		) / 1000,
	})

	return nil
}

func (t *HTTPTransport) Close() error {
	t.mu.Lock()

	if t.closed {
		t.mu.Unlock()
		return nil
	}

	t.closed = true
	t.cancel()
	t.mu.Unlock()

	<-t.done

	return nil
}
