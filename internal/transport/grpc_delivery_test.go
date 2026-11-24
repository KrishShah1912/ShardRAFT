package transport

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcDeliveryTestServer struct {
	shardraftpb.UnimplementedRaftServiceServer

	handler func(
		context.Context,
		*shardraftpb.StepRequest,
	) (*shardraftpb.StepResponse, error)
}

func (s *grpcDeliveryTestServer) Step(
	ctx context.Context,
	request *shardraftpb.StepRequest,
) (*shardraftpb.StepResponse, error) {
	return s.handler(ctx, request)
}

func startGRPCDeliveryTestServer(
	t *testing.T,
	handler func(
		context.Context,
		*shardraftpb.StepRequest,
	) (*shardraftpb.StepResponse, error),
) string {
	t.Helper()

	listener, err := net.Listen(
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}

	server := grpc.NewServer()

	shardraftpb.RegisterRaftServiceServer(
		server,
		&grpcDeliveryTestServer{
			handler: handler,
		},
	)

	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-done
	})

	return listener.Addr().String()
}

func newTestGRPCDelivery(
	t *testing.T,
	address string,
) *GRPCDelivery {
	t.Helper()

	delivery, err := NewGRPCDelivery(
		1,
		[]Peer{
			{
				ID:      2,
				Address: address,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = delivery.Close()
	})

	return delivery
}

func testGRPCMessage() raft.Message {
	return raft.Message{
		Type:      raft.MsgAppend,
		From:      1,
		To:        2,
		Term:      5,
		LogTerm:   4,
		Index:     10,
		Commit:    9,
		RequestID: 123,
		Entries: []raft.Entry{
			{
				Term:  5,
				Index: 11,
				Type:  raft.EntryNormal,
				Data:  []byte("test-command"),
			},
		},
	}
}

func TestGRPCDeliverySuccess(t *testing.T) {
	received := make(
		chan *shardraftpb.StepRequest,
		1,
	)

	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			request *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			received <- request

			return &shardraftpb.StepResponse{
				Accepted: true,
			}, nil
		},
	)

	delivery := newTestGRPCDelivery(t, address)

	message := testGRPCMessage()

	if err := delivery.Deliver(
		context.Background(),
		message,
	); err != nil {
		t.Fatal(err)
	}

	select {
	case request := <-received:
		if request.ShardId != 1 {
			t.Fatal("incorrect shard ID")
		}

		decoded, err := DecodeRaftMessage(
			request.Message,
		)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(message, decoded) {
			t.Fatal("delivered message mismatch")
		}

	case <-time.After(time.Second):
		t.Fatal("message was not received")
	}
}

func TestGRPCDeliveryRejected(t *testing.T) {
	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			request *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			return &shardraftpb.StepResponse{
				Accepted: false,
			}, nil
		},
	)

	delivery := newTestGRPCDelivery(t, address)

	if err := delivery.Deliver(
		context.Background(),
		testGRPCMessage(),
	); err == nil {
		t.Fatal("expected rejected delivery error")
	}
}

func TestGRPCDeliveryServerError(t *testing.T) {
	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			request *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			return nil, status.Error(
				codes.Unavailable,
				"peer unavailable",
			)
		},
	)

	delivery := newTestGRPCDelivery(t, address)

	err := delivery.Deliver(
		context.Background(),
		testGRPCMessage(),
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGRPCDeliveryCallerDeadline(t *testing.T) {
	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			request *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)

	delivery := newTestGRPCDelivery(t, address)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()

	started := time.Now()

	err := delivery.Deliver(
		ctx,
		testGRPCMessage(),
	)

	if status.Code(err) != codes.DeadlineExceeded &&
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error: %v", err)
	}

	if time.Since(started) > time.Second {
		t.Fatal("caller deadline was not respected")
	}
}

func TestGRPCDeliveryDefaultDeadline(t *testing.T) {
	address := startGRPCDeliveryTestServer(
		t,
		func(
			ctx context.Context,
			request *shardraftpb.StepRequest,
		) (*shardraftpb.StepResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)

	delivery := newTestGRPCDelivery(t, address)

	started := time.Now()

	err := delivery.Deliver(
		context.Background(),
		testGRPCMessage(),
	)

	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("unexpected error: %v", err)
	}

	if time.Since(started) > 4*time.Second {
		t.Fatal("default deadline was not respected")
	}
}

func TestGRPCDeliveryUnknownPeer(t *testing.T) {
	delivery, err := NewGRPCDelivery(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer delivery.Close()

	err = delivery.Deliver(
		context.Background(),
		testGRPCMessage(),
	)

	if !errors.Is(err, ErrPeer) {
		t.Fatalf("expected ErrPeer, got %v", err)
	}
}

func TestGRPCDeliveryClosed(t *testing.T) {
	delivery, err := NewGRPCDelivery(1, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := delivery.Close(); err != nil {
		t.Fatal(err)
	}

	if err := delivery.Close(); err != nil {
		t.Fatal(err)
	}

	err = delivery.Deliver(
		context.Background(),
		testGRPCMessage(),
	)

	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestGRPCDeliveryCanceledContext(t *testing.T) {
	delivery, err := NewGRPCDelivery(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer delivery.Close()

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	err = delivery.Deliver(
		ctx,
		testGRPCMessage(),
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGRPCDeliveryInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		shardID uint64
		peers   []Peer
	}{
		{
			name: "zero shard ID",
		},
		{
			name:    "zero peer ID",
			shardID: 1,
			peers: []Peer{
				{Address: "127.0.0.1:9092"},
			},
		},
		{
			name:    "empty address",
			shardID: 1,
			peers: []Peer{
				{ID: 2},
			},
		},
		{
			name:    "duplicate peer ID",
			shardID: 1,
			peers: []Peer{
				{ID: 2, Address: "127.0.0.1:9092"},
				{ID: 2, Address: "127.0.0.1:9093"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			delivery, err := NewGRPCDelivery(
				tc.shardID,
				tc.peers,
			)

			if err == nil {
				_ = delivery.Close()
				t.Fatal("expected configuration error")
			}
		})
	}
}
