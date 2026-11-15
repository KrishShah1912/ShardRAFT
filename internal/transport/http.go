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

type outgoing struct {
	message raft.Message
	address string
}

type peerWorker struct {
	address string
	queue   chan outgoing
}

type HTTPTransport struct {
	mu     sync.Mutex
	peers  map[raft.NodeID]*peerWorker
	client *http.Client
	cancel context.CancelFunc
	done   chan struct{}
	errors chan error
	closed bool
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
			return nil, fmt.Errorf("duplicate peer ID %d", peer.ID)
		}

		t.peers[peer.ID] = &peerWorker{
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

// Send validates destinations before enqueueing messages.
//
// Each peer has an independent queue. If one peer's queue is
// full, messages for other peers can still be accepted.
//
// ErrQueueFull indicates partial or complete rejection due
// to backpressure. Accepted messages remain ordered per peer.
func (t *HTTPTransport) Send(messages []raft.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrClosed
	}

	// Validate all destinations before accepting messages.
	for _, message := range messages {
		if _, ok := t.peers[message.To]; !ok {
			return fmt.Errorf("%w: %d", ErrPeer, message.To)
		}
	}

	var fullPeers []raft.NodeID
	seenFull := make(map[raft.NodeID]bool)

	for _, message := range messages {
		worker := t.peers[message.To]

		if len(worker.queue) == cap(worker.queue) {
			if !seenFull[message.To] {
				seenFull[message.To] = true
				fullPeers = append(fullPeers, message.To)
			}
			continue
		}

		worker.queue <- outgoing{
			message: message,
			address: worker.address,
		}
	}

	if len(fullPeers) > 0 {
		return fmt.Errorf("%w: peers %v", ErrQueueFull, fullPeers)
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
			if err := t.deliverWithRetry(ctx, item); err != nil {
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
	item outgoing,
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
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("peer returned HTTP %d", resp.StatusCode)
	}

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
