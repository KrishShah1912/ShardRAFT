package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvGetTestNode struct {
	nodeStatus raft.Status
	statusErr  error

	localRead func(string) (string, bool, error)

	linearRead func(
		context.Context,
		string,
	) (string, bool, error)
}

func (n *kvGetTestNode) StatusSnapshot() (
	raft.Status,
	uint64,
	error,
) {
	return n.nodeStatus, n.nodeStatus.Commit, n.statusErr
}

func (n *kvGetTestNode) Get(
	key string,
) (string, bool, error) {
	if n.localRead == nil {
		panic("unexpected local read")
	}

	return n.localRead(key)
}

func (n *kvGetTestNode) GetLinearizable(
	ctx context.Context,
	key string,
) (string, bool, error) {
	if n.linearRead == nil {
		panic("unexpected linearizable read")
	}

	return n.linearRead(ctx, key)
}

func (n *kvGetTestNode) Propose(
	command kvstore.Command,
) error {
	panic("Get must not propose mutations")
}

func (n *kvGetTestNode) CheckedRequestResult(
	command kvstore.Command,
) (kvstore.RequestResult, bool, error) {
	panic("Get must not access mutation deduplication")
}

var _ kvGRPCNode = (*kvGetTestNode)(nil)

func newKVGetRequest(
	mode shardraftpb.ReadMode,
) *shardraftpb.GetRequest {
	return &shardraftpb.GetRequest{
		Header: &shardraftpb.RequestHeader{
			ShardId: 1,
			Epoch:   1,
		},
		Key:      "alpha",
		ReadMode: mode,
	}
}

func TestKVGRPCGetSuccess(t *testing.T) {
	tests := []struct {
		name  string
		mode  shardraftpb.ReadMode
		role  raft.Role
		value string
	}{
		{
			"DefaultLinearizable",
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
			raft.Leader,
			"100",
		},
		{
			"LinearizableEmptyValue",
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
			raft.Leader,
			"",
		},
		{
			"LocalLeader",
			shardraftpb.ReadMode_READ_MODE_LOCAL,
			raft.Leader,
			"100",
		},
		{
			"LocalFollower",
			shardraftpb.ReadMode_READ_MODE_LOCAL,
			raft.Follower,
			"90",
		},
		{
			"LocalCandidate",
			shardraftpb.ReadMode_READ_MODE_LOCAL,
			raft.Candidate,
			"80",
		},
		{
			"LocalEmptyValue",
			shardraftpb.ReadMode_READ_MODE_LOCAL,
			raft.Follower,
			"",
		},
		{
			"UnicodeValue",
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
			raft.Leader,
			"hello世界",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := &kvGetTestNode{
				nodeStatus: raft.Status{
					ID:   1,
					Role: tc.role,
					Term: 3,
				},
			}

			calls := 0

			read := func(key string) (string, bool, error) {
				calls++

				if key != "alpha" {
					t.Fatalf("unexpected key: %q", key)
				}

				return tc.value, true, nil
			}

			if tc.mode == shardraftpb.ReadMode_READ_MODE_LOCAL {
				node.localRead = read
			} else {
				node.linearRead = func(
					ctx context.Context,
					key string,
				) (string, bool, error) {
					return read(key)
				}
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Get(
				context.Background(),
				newKVGetRequest(tc.mode),
			)

			if err != nil ||
				response == nil ||
				response.Error != nil ||
				!response.Found ||
				response.Key != "alpha" ||
				response.Value != tc.value ||
				calls != 1 {
				t.Fatalf(
					"response=%v err=%v calls=%d",
					response,
					err,
					calls,
				)
			}
		})
	}
}

func TestKVGRPCGetMissingKey(t *testing.T) {
	for _, mode := range []shardraftpb.ReadMode{
		shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		shardraftpb.ReadMode_READ_MODE_LOCAL,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			node := &kvGetTestNode{
				localRead: func(
					string,
				) (string, bool, error) {
					return "", false, nil
				},
				linearRead: func(
					context.Context,
					string,
				) (string, bool, error) {
					return "", false, nil
				},
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Get(
				context.Background(),
				newKVGetRequest(mode),
			)

			if err != nil ||
				response == nil ||
				response.Found ||
				response.Value != "" ||
				response.Key != "alpha" {
				t.Fatalf("response=%v err=%v", response, err)
			}

			requireKVValidationCode(
				t,
				response.Error,
				shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND,
			)
		})
	}
}

func TestKVGRPCGetErrors(t *testing.T) {
	unavailable := shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE
	internal := shardraftpb.ErrorCode_ERROR_CODE_INTERNAL

	tests := []struct {
		name     string
		err      error
		expected shardraftpb.ErrorCode
	}{
		{"NotLeader", raft.ErrNotLeader, shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER},
		{"ReadNotReady", raft.ErrReadNotReady, unavailable},
		{"RaftCapacity", raft.ErrTooManyPendingReads, unavailable},
		{"ShardCapacity", shard.ErrTooManyPendingReads, unavailable},
		{"Closed", shard.ErrClosed, unavailable},
		{"Stopped", shard.ErrStopped, unavailable},
		{"InternalDeadline", context.DeadlineExceeded, unavailable},
		{"InternalCancellation", context.Canceled, unavailable},
		{"InvalidKey", shard.ErrInvalidReadKey, shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST},
		{"InvalidConfirmation", shard.ErrInvalidReadConfirmation, internal},
		{"Unexpected", errors.New("private storage details"), internal},
		{"WrappedNotLeader", fmt.Errorf("wrapped: %w", raft.ErrNotLeader), shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := &kvGetTestNode{
				nodeStatus: raft.Status{
					ID:       1,
					Role:     raft.Follower,
					Term:     7,
					LeaderID: 2,
				},
				linearRead: func(
					context.Context,
					string,
				) (string, bool, error) {
					return "must not leak", true, tc.err
				},
			}

			server := newKVGRPCServer(
				1,
				node,
				map[raft.NodeID]string{
					2: "127.0.0.1:19092",
				},
			)

			response, err := server.Get(
				context.Background(),
				newKVGetRequest(
					shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
				),
			)

			if err != nil ||
				response == nil ||
				response.Found ||
				response.Value != "" {
				t.Fatalf("response=%v err=%v", response, err)
			}

			requireKVValidationCode(
				t,
				response.Error,
				tc.expected,
			)

			if response.Error.LeaderId != 2 ||
				response.Error.LeaderAddress != "127.0.0.1:19092" ||
				response.Error.Term != 7 {
				t.Fatalf("incorrect metadata: %v", response.Error)
			}

			if response.Error.Message == tc.err.Error() &&
				tc.name == "Unexpected" {
				t.Fatal("internal error leaked")
			}
		})
	}
}

func TestKVGRPCGetInvalidRequests(t *testing.T) {
	server := newKVGRPCServer(
		1,
		&kvGetTestNode{},
		nil,
	)

	invalid := shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST
	wrong := shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD

	tests := []struct {
		name     string
		request  *shardraftpb.GetRequest
		expected shardraftpb.ErrorCode
	}{
		{"Nil", nil, invalid},
		{"MissingHeader", &shardraftpb.GetRequest{Key: "alpha"}, invalid},
		{"EmptyKey", &shardraftpb.GetRequest{Header: &shardraftpb.RequestHeader{ShardId: 1}}, invalid},
		{"WrongShard", &shardraftpb.GetRequest{Header: &shardraftpb.RequestHeader{ShardId: 2}, Key: "alpha"}, wrong},
		{"WrongEpoch", &shardraftpb.GetRequest{Header: &shardraftpb.RequestHeader{ShardId: 1, Epoch: 2}, Key: "alpha"}, wrong},
		{"UnknownMode", newKVGetRequest(shardraftpb.ReadMode(99)), invalid},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			response, err := server.Get(
				context.Background(),
				tc.request,
			)

			if err != nil || response == nil {
				t.Fatalf("response=%v err=%v", response, err)
			}

			requireKVValidationCode(
				t,
				response.Error,
				tc.expected,
			)
		})
	}
}

func TestKVGRPCGetUnavailableNode(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	response, err := server.Get(
		context.Background(),
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		),
	)

	if err != nil || response == nil {
		t.Fatalf("response=%v err=%v", response, err)
	}

	requireKVValidationCode(
		t,
		response.Error,
		shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
	)
}

func TestKVGRPCGetUnavailableStatus(t *testing.T) {
	node := &kvGetTestNode{
		statusErr: shard.ErrClosed,
		linearRead: func(
			context.Context,
			string,
		) (string, bool, error) {
			return "", false, shard.ErrClosed
		},
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Get(
		context.Background(),
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		),
	)

	if err != nil || response == nil {
		t.Fatalf("response=%v err=%v", response, err)
	}

	requireKVValidationCode(
		t,
		response.Error,
		shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
	)

	if response.Error.LeaderId != 0 ||
		response.Error.LeaderAddress != "" ||
		response.Error.Term != 0 {
		t.Fatalf("unexpected metadata: %v", response.Error)
	}
}

func TestKVGRPCGetLocalFailure(t *testing.T) {
	node := &kvGetTestNode{
		localRead: func(
			string,
		) (string, bool, error) {
			return "", false, shard.ErrStopped
		},
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Get(
		context.Background(),
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LOCAL,
		),
	)

	if err != nil || response == nil {
		t.Fatalf("response=%v err=%v", response, err)
	}

	requireKVValidationCode(
		t,
		response.Error,
		shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
	)
}

func TestKVGRPCGetInvalidStoredValue(t *testing.T) {
	node := &kvGetTestNode{
		localRead: func(
			string,
		) (string, bool, error) {
			return "\xff", true, nil
		},
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Get(
		context.Background(),
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LOCAL,
		),
	)

	if err != nil ||
		response == nil ||
		response.Found ||
		response.Value != "" {
		t.Fatalf("response=%v err=%v", response, err)
	}

	requireKVValidationCode(
		t,
		response.Error,
		shardraftpb.ErrorCode_ERROR_CODE_INTERNAL,
	)
}

func TestKVGRPCGetCanceledBeforeRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server := newKVGRPCServer(
		1,
		&kvGetTestNode{},
		nil,
	)

	response, err := server.Get(
		ctx,
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		),
	)

	if response != nil ||
		status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCGetExpiredDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	server := newKVGRPCServer(
		1,
		&kvGetTestNode{},
		nil,
	)

	response, err := server.Get(
		ctx,
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		),
	)

	if response != nil ||
		status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCGetCanceledDuringRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := &kvGetTestNode{
		linearRead: func(
			readCtx context.Context,
			key string,
		) (string, bool, error) {
			cancel()
			return "100", true, readCtx.Err()
		},
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Get(
		ctx,
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		),
	)

	if response != nil ||
		status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCGetDeadlineDuringRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()

	node := &kvGetTestNode{
		linearRead: func(
			readCtx context.Context,
			key string,
		) (string, bool, error) {
			<-readCtx.Done()
			return "", false, readCtx.Err()
		},
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Get(
		ctx,
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		),
	)

	if response != nil ||
		status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCGetCanceledLocalRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := &kvGetTestNode{
		localRead: func(
			string,
		) (string, bool, error) {
			cancel()
			return "100", true, nil
		},
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Get(
		ctx,
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LOCAL,
		),
	)

	if response != nil ||
		status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCGetConcurrentReads(t *testing.T) {
	node := &kvGetTestNode{
		linearRead: func(
			ctx context.Context,
			key string,
		) (string, bool, error) {
			return key, true, nil
		},
	}

	server := newKVGRPCServer(1, node, nil)

	const count = 64

	var wg sync.WaitGroup

	start := make(chan struct{})
	failures := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			<-start

			request := newKVGetRequest(
				shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
			)

			request.Key = fmt.Sprintf("key-%d", index)

			response, err := server.Get(
				context.Background(),
				request,
			)

			if err != nil ||
				response == nil ||
				response.Error != nil ||
				!response.Found ||
				response.Key != request.Key ||
				response.Value != request.Key {
				failures <- fmt.Errorf(
					"request %d: response=%v err=%v",
					index,
					response,
					err,
				)
			}
		}(i)
	}

	close(start)
	wg.Wait()
	close(failures)

	for err := range failures {
		t.Error(err)
	}
}
