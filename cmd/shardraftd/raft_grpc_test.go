package main

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	"github.com/KrishShah1912/shardraft/internal/transport"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcReceiverTestNode struct {
	nodeStatus raft.Status
	statusErr  error
	stepErr    error
	messages   []raft.Message
}

func (n *grpcReceiverTestNode) Status() (raft.Status, error) {
	return n.nodeStatus, n.statusErr
}

func (n *grpcReceiverTestNode) Step(message raft.Message) error {
	n.messages = append(n.messages, message)
	return n.stepErr
}

func grpcReceiverTestRequest() *shardraftpb.StepRequest {
	return &shardraftpb.StepRequest{
		ShardId: 1,
		Message: &shardraftpb.RaftMessage{
			Type:      shardraftpb.MessageType_MSG_APPEND,
			From:      1,
			To:        2,
			Term:      5,
			LogTerm:   4,
			Index:     10,
			Commit:    9,
			RequestId: 123,
			Entries: []*shardraftpb.LogEntry{
				{
					Term:  5,
					Index: 11,
					Type:  shardraftpb.EntryType_ENTRY_NORMAL,
					Data:  []byte("test-command"),
				},
			},
		},
	}
}

func TestRaftGRPCReceiverSuccess(t *testing.T) {
	node := &grpcReceiverTestNode{
		nodeStatus: raft.Status{ID: 2},
	}

	server := newRaftGRPCServer(
		1, node, []raft.NodeID{1, 3},
	)

	request := grpcReceiverTestRequest()

	response, err := server.Step(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}

	if response == nil || !response.Accepted {
		t.Fatal("expected accepted response")
	}

	if len(node.messages) != 1 {
		t.Fatal("expected exactly one processed message")
	}

	expected, err := transport.DecodeRaftMessage(
		request.Message,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expected, node.messages[0]) {
		t.Fatal("processed message mismatch")
	}
}

func TestRaftGRPCReceiverValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*shardraftpb.StepRequest)
		want   codes.Code
	}{
		{
			name: "missing message",
			change: func(r *shardraftpb.StepRequest) {
				r.Message = nil
			},
			want: codes.InvalidArgument,
		},
		{
			name: "zero shard",
			change: func(r *shardraftpb.StepRequest) {
				r.ShardId = 0
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "wrong shard",
			change: func(r *shardraftpb.StepRequest) {
				r.ShardId = 99
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "zero sender",
			change: func(r *shardraftpb.StepRequest) {
				r.Message.From = 0
			},
			want: codes.InvalidArgument,
		},
		{
			name: "zero destination",
			change: func(r *shardraftpb.StepRequest) {
				r.Message.To = 0
			},
			want: codes.InvalidArgument,
		},
		{
			name: "self message",
			change: func(r *shardraftpb.StepRequest) {
				r.Message.From = 2
			},
			want: codes.InvalidArgument,
		},
		{
			name: "wrong destination",
			change: func(r *shardraftpb.StepRequest) {
				r.Message.To = 3
			},
			want: codes.InvalidArgument,
		},
		{
			name: "unknown peer",
			change: func(r *shardraftpb.StepRequest) {
				r.Message.From = 99
			},
			want: codes.PermissionDenied,
		},
		{
			name: "invalid message type",
			change: func(r *shardraftpb.StepRequest) {
				r.Message.Type = shardraftpb.MessageType(99)
			},
			want: codes.InvalidArgument,
		},
		{
			name: "invalid entry type",
			change: func(r *shardraftpb.StepRequest) {
				r.Message.Entries[0].Type =
					shardraftpb.EntryType(99)
			},
			want: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := &grpcReceiverTestNode{
				nodeStatus: raft.Status{ID: 2},
			}

			server := newRaftGRPCServer(
				1, node, []raft.NodeID{1, 3},
			)

			request := grpcReceiverTestRequest()
			tc.change(request)

			response, err := server.Step(
				context.Background(),
				request,
			)

			if status.Code(err) != tc.want {
				t.Fatalf(
					"want %v, got %v",
					tc.want, err,
				)
			}

			if response != nil {
				t.Fatal("invalid request was acknowledged")
			}

			if len(node.messages) != 0 {
				t.Fatal("invalid request reached Raft")
			}
		})
	}
}

func TestRaftGRPCReceiverNilRequest(t *testing.T) {
	node := &grpcReceiverTestNode{
		nodeStatus: raft.Status{ID: 2},
	}

	server := newRaftGRPCServer(
		1, node, []raft.NodeID{1},
	)

	_, err := server.Step(context.Background(), nil)

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRaftGRPCReceiverUnavailableNode(t *testing.T) {
	node := &grpcReceiverTestNode{
		statusErr: errors.New("node unavailable"),
	}

	server := newRaftGRPCServer(
		1, node, []raft.NodeID{1},
	)

	_, err := server.Step(
		context.Background(),
		grpcReceiverTestRequest(),
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(node.messages) != 0 {
		t.Fatal("unavailable node processed message")
	}
}

func TestRaftGRPCReceiverProcessingErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "closed shard",
			err:  shard.ErrClosed,
			want: codes.Unavailable,
		},
		{
			name: "stopped shard",
			err:  shard.ErrStopped,
			want: codes.Unavailable,
		},
		{
			name: "internal error",
			err:  errors.New("processing failed"),
			want: codes.Internal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := &grpcReceiverTestNode{
				nodeStatus: raft.Status{ID: 2},
				stepErr:    tc.err,
			}

			server := newRaftGRPCServer(
				1, node, []raft.NodeID{1},
			)

			response, err := server.Step(
				context.Background(),
				grpcReceiverTestRequest(),
			)

			if status.Code(err) != tc.want {
				t.Fatalf("unexpected error: %v", err)
			}

			if response != nil {
				t.Fatal("failed message was acknowledged")
			}
		})
	}
}

func TestRaftGRPCReceiverCanceledContext(t *testing.T) {
	node := &grpcReceiverTestNode{
		nodeStatus: raft.Status{ID: 2},
	}

	server := newRaftGRPCServer(
		1, node, []raft.NodeID{1},
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, err := server.Step(
		ctx,
		grpcReceiverTestRequest(),
	)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(node.messages) != 0 {
		t.Fatal("canceled request reached Raft")
	}
}

func TestRaftGRPCReceiverWireIntegration(t *testing.T) {
	node := &grpcReceiverTestNode{
		nodeStatus: raft.Status{ID: 2},
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := grpc.NewServer()

	shardraftpb.RegisterRaftServiceServer(
		server,
		newRaftGRPCServer(
			1, node, []raft.NodeID{1},
		),
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

	delivery, err := transport.NewGRPCDelivery(
		1,
		[]transport.Peer{
			{
				ID:      2,
				Address: listener.Addr().String(),
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer delivery.Close()

	message, err := transport.DecodeRaftMessage(
		grpcReceiverTestRequest().Message,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	if err := delivery.Deliver(ctx, message); err != nil {
		t.Fatal(err)
	}

	// GracefulStop waits for the RPC handler to finish.
	server.GracefulStop()
	<-done

	if len(node.messages) != 1 {
		t.Fatal("expected one delivered message")
	}

	if !reflect.DeepEqual(message, node.messages[0]) {
		t.Fatal("wire integration message mismatch")
	}
}
