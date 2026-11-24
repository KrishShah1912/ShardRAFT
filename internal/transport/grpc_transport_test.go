package transport

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newQueuedGRPCTestTransport(
	t *testing.T,
	peers []Peer,
	queueSize int,
) *GRPCTransport {
	t.Helper()

	tr, err := NewGRPCTransport(1, peers, queueSize)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = tr.Close()
	})

	return tr
}

func waitGRPCTransportCondition(
	t *testing.T,
	condition func() bool,
) {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}

		select {
		case <-timer.C:
			t.Fatal("transport condition timed out")
		case <-ticker.C:
		}
	}
}

func TestGRPCTransportQueuedDelivery(t *testing.T) {
	received := make(chan raft.Message, 1)

	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			message, err := DecodeRaftMessage(req.Message)
			if err != nil {
				return nil, err
			}

			received <- message

			return &shardraftpb.StepResponse{
				Accepted: true,
			}, nil
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{{ID: 2, Address: address}},
		8,
	)

	message := testGRPCMessage()

	if err := tr.Send([]raft.Message{message}); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-received:
		if !reflect.DeepEqual(message, got) {
			t.Fatal("queued message mismatch")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued delivery timed out")
	}

	waitGRPCTransportCondition(t, func() bool {
		return tr.Stats().Delivered == 1
	})
}

func TestGRPCTransportOrdering(t *testing.T) {
	received := make(chan uint64, 10)

	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			received <- req.Message.RequestId

			return &shardraftpb.StepResponse{
				Accepted: true,
			}, nil
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{{ID: 2, Address: address}},
		16,
	)

	var messages []raft.Message

	for i := uint64(1); i <= 10; i++ {
		m := testGRPCMessage()
		m.RequestID = i
		messages = append(messages, m)
	}

	if err := tr.Send(messages); err != nil {
		t.Fatal(err)
	}

	for expected := uint64(1); expected <= 10; expected++ {
		select {
		case got := <-received:
			if got != expected {
				t.Fatalf(
					"expected request %d, got %d",
					expected,
					got,
				)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("ordered delivery timed out")
		}
	}
}

func TestGRPCTransportRetryRecovery(t *testing.T) {
	var attempts atomic.Int32

	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			if attempts.Add(1) < 3 {
				return nil, status.Error(
					codes.Unavailable,
					"temporary failure",
				)
			}

			return &shardraftpb.StepResponse{
				Accepted: true,
			}, nil
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{{ID: 2, Address: address}},
		8,
	)

	if err := tr.Send([]raft.Message{
		testGRPCMessage(),
	}); err != nil {
		t.Fatal(err)
	}

	waitGRPCTransportCondition(t, func() bool {
		return tr.Stats().Delivered == 1
	})

	stats := tr.Stats()

	if attempts.Load() != 3 ||
		stats.Sent != 3 ||
		stats.Retries != 2 ||
		stats.Failures != 0 {
		t.Fatalf("unexpected retry statistics: %+v", stats)
	}
}

func TestGRPCTransportRetryExhaustion(t *testing.T) {
	var attempts atomic.Int32

	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			attempts.Add(1)

			return nil, status.Error(
				codes.Unavailable,
				"peer unavailable",
			)
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{{ID: 2, Address: address}},
		8,
	)

	if err := tr.Send([]raft.Message{
		testGRPCMessage(),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-tr.Errors():
		if err == nil {
			t.Fatal("expected delivery failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry exhaustion was not reported")
	}

	stats := tr.Stats()

	if attempts.Load() != int32(maxDeliveryAttempts) ||
		stats.Sent != uint64(maxDeliveryAttempts) ||
		stats.Retries != uint64(maxDeliveryAttempts-1) ||
		stats.Failures != 1 ||
		stats.Delivered != 0 {
		t.Fatalf("unexpected failure statistics: %+v", stats)
	}
}

func TestGRPCTransportPeerIsolation(t *testing.T) {
	started := make(chan struct{}, 1)
	received := make(chan struct{}, 1)

	slow := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			select {
			case started <- struct{}{}:
			default:
			}

			<-ctx.Done()
			return nil, ctx.Err()
		},
	)

	fast := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			received <- struct{}{}

			return &shardraftpb.StepResponse{
				Accepted: true,
			}, nil
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{
			{ID: 2, Address: slow},
			{ID: 3, Address: fast},
		},
		8,
	)

	if err := tr.Send([]raft.Message{
		{Type: raft.MsgAppend, From: 1, To: 2},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("slow peer did not start")
	}

	if err := tr.Send([]raft.Message{
		{Type: raft.MsgVote, From: 1, To: 3},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("slow peer blocked healthy peer")
	}
}

func TestGRPCTransportQueueIsolation(t *testing.T) {
	started := make(chan struct{}, 1)
	received := make(chan struct{}, 1)
	release := make(chan struct{})

	slow := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			select {
			case started <- struct{}{}:
			default:
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return &shardraftpb.StepResponse{
					Accepted: true,
				}, nil
			}
		},
	)

	fast := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			received <- struct{}{}

			return &shardraftpb.StepResponse{
				Accepted: true,
			}, nil
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{
			{ID: 2, Address: slow},
			{ID: 3, Address: fast},
		},
		1,
	)

	defer close(release)

	slowMessage := raft.Message{
		Type: raft.MsgAppend,
		From: 1,
		To:   2,
	}

	if err := tr.Send([]raft.Message{
		slowMessage,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("slow peer did not start")
	}

	if err := tr.Send([]raft.Message{
		slowMessage,
	}); err != nil {
		t.Fatal(err)
	}

	err := tr.Send([]raft.Message{
		slowMessage,
		{Type: raft.MsgVote, From: 1, To: 3},
	})

	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("full peer queue blocked healthy peer")
	}

	if tr.Stats().Backpressure != 1 {
		t.Fatal("expected one backpressure event")
	}
}

func TestGRPCTransportStatistics(t *testing.T) {
	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			return &shardraftpb.StepResponse{
				Accepted: true,
			}, nil
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{{ID: 2, Address: address}},
		8,
	)

	if err := tr.Send([]raft.Message{
		testGRPCMessage(),
		testGRPCMessage(),
	}); err != nil {
		t.Fatal(err)
	}

	waitGRPCTransportCondition(t, func() bool {
		return tr.Stats().Delivered == 2
	})

	stats := tr.Stats()

	if stats.Enqueued != 2 ||
		stats.Sent != 2 ||
		stats.Delivered != 2 ||
		stats.Failures != 0 ||
		stats.Retries != 0 {
		t.Fatalf("unexpected statistics: %+v", stats)
	}

	if len(stats.Peers) != 1 {
		t.Fatal("expected one peer")
	}

	peer := stats.Peers[0]

	if peer.PeerID != 2 ||
		peer.Address != address ||
		peer.QueueCapacity != 8 ||
		peer.QueueDepth != 0 ||
		peer.LastSuccessAt == nil {
		t.Fatalf("unexpected peer statistics: %+v", peer)
	}
}

func TestGRPCTransportEvents(t *testing.T) {
	var attempts atomic.Int32
	var mu sync.Mutex

	events := make(map[string]int)

	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			if attempts.Add(1) == 1 {
				return nil, status.Error(
					codes.Unavailable,
					"temporary failure",
				)
			}

			return &shardraftpb.StepResponse{
				Accepted: true,
			}, nil
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{{ID: 2, Address: address}},
		8,
	)

	tr.SetEventHandler(func(event Event) {
		mu.Lock()
		defer mu.Unlock()

		events[event.Type]++
	})

	if err := tr.Send([]raft.Message{
		testGRPCMessage(),
	}); err != nil {
		t.Fatal(err)
	}

	waitGRPCTransportCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return events[EventDelivered] == 1
	})

	mu.Lock()
	defer mu.Unlock()

	if events[EventQueued] != 1 ||
		events[EventSent] != 2 ||
		events[EventRetry] != 1 ||
		events[EventDelivered] != 1 ||
		events[EventFailure] != 0 {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestGRPCTransportShutdownDuringRPC(t *testing.T) {
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)

	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			req *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			started <- struct{}{}

			<-ctx.Done()

			canceled <- struct{}{}

			return nil, ctx.Err()
		},
	)

	tr := newQueuedGRPCTestTransport(
		t,
		[]Peer{{ID: 2, Address: address}},
		8,
	)

	if err := tr.Send([]raft.Message{
		testGRPCMessage(),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("RPC did not start")
	}

	done := make(chan error, 1)

	go func() {
		done <- tr.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("transport shutdown timed out")
	}

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("active RPC was not canceled")
	}

	if !tr.Stats().Closed {
		t.Fatal("transport should be closed")
	}

	if err := tr.Send(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestGRPCTransportConnectionCleanup(t *testing.T) {
	delivery, err := NewGRPCDelivery(
		1,
		[]Peer{{ID: 2, Address: "127.0.0.1:9092"}},
	)
	if err != nil {
		t.Fatal(err)
	}

	defer delivery.Close()

	tr, err := newQueuedTransport(
		[]Peer{{ID: 2, Address: "127.0.0.1:9092"}},
		8,
		func(ctx context.Context, item outgoing) error {
			return delivery.Deliver(ctx, item.message)
		},
		delivery.Close,
	)
	if err != nil {
		t.Fatal(err)
	}

	defer tr.Close()

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := tr.Close(); err != nil {
				t.Errorf("close failed: %v", err)
			}
		}()
	}

	wg.Wait()

	err = delivery.Deliver(
		context.Background(),
		testGRPCMessage(),
	)

	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected closed delivery, got %v", err)
	}
}

func TestGRPCTransportUnknownPeer(t *testing.T) {
	tr := newQueuedGRPCTestTransport(t, nil, 8)

	err := tr.Send([]raft.Message{
		{Type: raft.MsgVote, From: 1, To: 99},
	})

	if !errors.Is(err, ErrPeer) {
		t.Fatalf("expected ErrPeer, got %v", err)
	}

	if tr.Stats().Enqueued != 0 {
		t.Fatal("unknown peer message was enqueued")
	}
}

func TestGRPCTransportInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		shardID   uint64
		peers     []Peer
		queueSize int
	}{
		{
			name:      "zero queue",
			shardID:   1,
			queueSize: 0,
		},
		{
			name:      "zero shard",
			queueSize: 8,
		},
		{
			name:      "invalid peer",
			shardID:   1,
			queueSize: 8,
			peers: []Peer{
				{ID: 0, Address: "127.0.0.1:9092"},
			},
		},
		{
			name:      "duplicate peer",
			shardID:   1,
			queueSize: 8,
			peers: []Peer{
				{ID: 2, Address: "127.0.0.1:9092"},
				{ID: 2, Address: "127.0.0.1:9093"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr, err := NewGRPCTransport(
				tc.shardID,
				tc.peers,
				tc.queueSize,
			)

			if err == nil {
				_ = tr.Close()
				t.Fatal("expected configuration error")
			}
		})
	}
}
