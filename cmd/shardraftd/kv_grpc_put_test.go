package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvMutationTestNode struct {
	kvGetTestNode

	propose func(kvstore.Command) error

	checkedResult func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error)
}

func (n *kvMutationTestNode) Propose(
	command kvstore.Command,
) error {
	if n.propose == nil {
		panic("unexpected proposal")
	}

	return n.propose(command)
}

func (n *kvMutationTestNode) CheckedRequestResult(
	command kvstore.Command,
) (kvstore.RequestResult, bool, error) {
	if n.checkedResult == nil {
		panic("unexpected result lookup")
	}

	return n.checkedResult(command)
}

var _ kvGRPCNode = (*kvMutationTestNode)(nil)

func newKVMutationTestNode() *kvMutationTestNode {
	return &kvMutationTestNode{
		kvGetTestNode: kvGetTestNode{
			nodeStatus: raft.Status{
				ID:       1,
				Role:     raft.Leader,
				Term:     3,
				LeaderID: 1,
			},
		},
	}
}

func newKVPutRequest() *shardraftpb.PutRequest {
	return &shardraftpb.PutRequest{
		Header: &shardraftpb.RequestHeader{
			ShardId:   1,
			Epoch:     1,
			ClientId:  "client-1",
			RequestId: "request-1",
		},
		Key:   "alpha",
		Value: "100",
	}
}

func TestKVGRPCPutSuccess(t *testing.T) {
	for _, value := range []string{
		"100",
		"",
		"hello世界",
	} {
		t.Run("Value="+value, func(t *testing.T) {
			node := newKVMutationTestNode()

			request := newKVPutRequest()
			request.Value = value

			proposals := 0
			lookups := 0

			verify := func(command kvstore.Command) {
				if command.Op != kvstore.OpPut ||
					command.Key != request.Key ||
					command.Value != request.Value ||
					command.ClientID != request.Header.ClientId ||
					command.RequestID != request.Header.RequestId {
					t.Fatalf("incorrect command: %+v", command)
				}
			}

			node.propose = func(command kvstore.Command) error {
				verify(command)
				proposals++
				return nil
			}

			node.checkedResult = func(
				command kvstore.Command,
			) (kvstore.RequestResult, bool, error) {
				verify(command)
				lookups++

				if proposals == 0 || lookups < 3 {
					return kvstore.RequestResult{}, false, nil
				}

				return kvstore.RequestResult{
					Applied: true,
					Value:   value,
				}, true, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Put(
				context.Background(),
				request,
			)

			if err != nil ||
				response == nil ||
				response.Error != nil ||
				!response.Applied ||
				response.Value != value ||
				proposals != 1 ||
				lookups < 3 {
				t.Fatalf(
					"response=%v err=%v proposals=%d lookups=%d",
					response, err, proposals, lookups,
				)
			}
		})
	}
}

func TestKVGRPCPutDuplicate(t *testing.T) {
	node := newKVMutationTestNode()

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		return kvstore.RequestResult{
			Applied: true,
			Value:   "100",
		}, true, nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Put(
		context.Background(),
		newKVPutRequest(),
	)

	if err != nil ||
		response == nil ||
		response.Error != nil ||
		!response.Applied ||
		response.Value != "100" {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCPutPending(t *testing.T) {
	node := newKVMutationTestNode()

	lookups := 0
	proposals := 0

	node.propose = func(kvstore.Command) error {
		proposals++
		return shard.ErrRequestPending
	}

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		lookups++

		if lookups < 3 {
			return kvstore.RequestResult{}, false, nil
		}

		return kvstore.RequestResult{
			Applied: true,
			Value:   "100",
		}, true, nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Put(
		context.Background(),
		newKVPutRequest(),
	)

	if err != nil ||
		response == nil ||
		response.Error != nil ||
		!response.Applied ||
		proposals != 1 {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCPutInvalidRequests(t *testing.T) {
	invalid := shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST
	wrong := shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD

	tests := []struct {
		name     string
		change   func(*shardraftpb.PutRequest) *shardraftpb.PutRequest
		expected shardraftpb.ErrorCode
	}{
		{"Nil", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			return nil
		}, invalid},
		{"MissingHeader", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Header = nil
			return r
		}, invalid},
		{"ZeroShard", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Header.ShardId = 0
			return r
		}, invalid},
		{"WrongShard", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Header.ShardId = 2
			return r
		}, wrong},
		{"WrongEpoch", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Header.Epoch = 2
			return r
		}, wrong},
		{"EmptyKey", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Key = ""
			return r
		}, invalid},
		{"MissingClient", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Header.ClientId = ""
			return r
		}, invalid},
		{"MissingRequest", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Header.RequestId = ""
			return r
		}, invalid},
		{"MissingBothIDs", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Header.ClientId = ""
			r.Header.RequestId = ""
			return r
		}, invalid},
		{"InvalidValue", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Value = "\xff"
			return r
		}, invalid},
		{"Oversized", func(r *shardraftpb.PutRequest) *shardraftpb.PutRequest {
			r.Value = strings.Repeat("a", kvMaxRequestSize+1)
			return r
		}, invalid},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newKVMutationTestNode()
			server := newKVGRPCServer(1, node, nil)

			response, err := server.Put(
				context.Background(),
				tc.change(newKVPutRequest()),
			)

			if err != nil || response == nil || response.Applied {
				t.Fatalf("response=%v err=%v", response, err)
			}

			requireKVValidationCode(
				t, response.Error, tc.expected,
			)
		})
	}
}

func TestKVGRPCPutNonLeader(t *testing.T) {
	for _, role := range []raft.Role{
		raft.Follower,
		raft.Candidate,
	} {
		node := newKVMutationTestNode()
		node.nodeStatus.Role = role
		node.nodeStatus.LeaderID = 2

		server := newKVGRPCServer(
			1,
			node,
			map[raft.NodeID]string{
				2: "127.0.0.1:19092",
			},
		)

		response, err := server.Put(
			context.Background(),
			newKVPutRequest(),
		)

		if err != nil || response == nil || response.Applied {
			t.Fatalf("response=%v err=%v", response, err)
		}

		requireKVValidationCode(
			t,
			response.Error,
			shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
		)

		if response.Error.LeaderId != 2 ||
			response.Error.LeaderAddress != "127.0.0.1:19092" ||
			response.Error.Term != 3 {
			t.Fatalf("incorrect routing metadata: %v", response.Error)
		}
	}
}

func TestKVGRPCPutUnavailable(t *testing.T) {
	node := newKVMutationTestNode()
	node.statusErr = shard.ErrClosed

	for _, backend := range []kvGRPCNode{nil, node} {
		server := newKVGRPCServer(1, backend, nil)

		response, err := server.Put(
			context.Background(),
			newKVPutRequest(),
		)

		if err != nil || response == nil || response.Applied {
			t.Fatalf("response=%v err=%v", response, err)
		}

		requireKVValidationCode(
			t,
			response.Error,
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
		)
	}
}

func TestKVGRPCPutMutationErrors(t *testing.T) {
	conflict := shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT
	unavailable := shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE
	unknown := shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN
	notLeader := shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER
	internal := shardraftpb.ErrorCode_ERROR_CODE_INTERNAL

	tests := []struct {
		name     string
		stage    string
		failure  error
		expected shardraftpb.ErrorCode
	}{
		{"LookupConflict", "lookup", kvstore.ErrRequestConflict, conflict},
		{"LookupClosed", "lookup", shard.ErrClosed, unavailable},
		{"LookupStopped", "lookup", shard.ErrStopped, unavailable},
		{"LookupNotLeader", "lookup", raft.ErrNotLeader, notLeader},
		{"LookupUnexpected", "lookup", errors.New("private storage error"), internal},
		{"ProposalConflict", "proposal", kvstore.ErrRequestConflict, conflict},
		{"ProposalNotLeader", "proposal", raft.ErrNotLeader, notLeader},
		{"ProposalStopped", "proposal", shard.ErrStopped, unknown},
		{"ProposalUnexpected", "proposal", errors.New("private WAL error"), unknown},
		{"AppliedLookupConflict", "confirmation", kvstore.ErrRequestConflict, conflict},
		{"AppliedLookupClosed", "confirmation", shard.ErrClosed, unknown},
		{"AppliedLookupNotLeader", "confirmation", raft.ErrNotLeader, unknown},
		{"AppliedLookupUnexpected", "confirmation", errors.New("private storage error"), unknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newKVMutationTestNode()

			proposals := 0

			node.propose = func(kvstore.Command) error {
				proposals++

				if tc.stage == "proposal" {
					return tc.failure
				}

				return nil
			}

			node.checkedResult = func(
				kvstore.Command,
			) (kvstore.RequestResult, bool, error) {
				if tc.stage == "lookup" ||
					(tc.stage == "confirmation" && proposals > 0) {
					return kvstore.RequestResult{}, false, tc.failure
				}

				return kvstore.RequestResult{}, false, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Put(
				context.Background(),
				newKVPutRequest(),
			)

			if err != nil ||
				response == nil ||
				response.Applied ||
				response.Value != "" {
				t.Fatalf("response=%v err=%v", response, err)
			}

			requireKVValidationCode(
				t, response.Error, tc.expected,
			)

			if strings.Contains(response.Error.Message, "private") {
				t.Fatal("internal error leaked")
			}

			if tc.stage == "lookup" && proposals != 0 {
				t.Fatal("proposal submitted after failed lookup")
			}
		})
	}
}

func TestKVGRPCPutCanceledBeforeSubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server := newKVGRPCServer(
		1,
		newKVMutationTestNode(),
		nil,
	)

	response, err := server.Put(ctx, newKVPutRequest())

	if response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCPutCanceledDuringLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := newKVMutationTestNode()

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		cancel()
		return kvstore.RequestResult{}, false, nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Put(ctx, newKVPutRequest())

	if response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCPutCanceledAfterSubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := newKVMutationTestNode()

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		return kvstore.RequestResult{}, false, nil
	}

	node.propose = func(kvstore.Command) error {
		cancel()
		return nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Put(ctx, newKVPutRequest())

	if response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCPutDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Millisecond,
	)
	defer cancel()

	node := newKVMutationTestNode()

	node.propose = func(kvstore.Command) error {
		return nil
	}

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		return kvstore.RequestResult{}, false, nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Put(ctx, newKVPutRequest())

	if response != nil ||
		status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCPutCommitTimeout(t *testing.T) {
	node := newKVMutationTestNode()

	node.propose = func(kvstore.Command) error {
		return nil
	}

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		return kvstore.RequestResult{}, false, nil
	}

	server := newKVGRPCServer(1, node, nil)

	start := time.Now()

	response, err := server.Put(
		context.Background(),
		newKVPutRequest(),
	)

	if err != nil ||
		response == nil ||
		response.Applied {
		t.Fatalf("response=%v err=%v", response, err)
	}

	requireKVValidationCode(
		t,
		response.Error,
		shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
	)

	if time.Since(start) < kvMutationTimeout {
		t.Fatal("mutation timed out prematurely")
	}
}

func TestKVGRPCPutInvalidStoredResult(t *testing.T) {
	node := newKVMutationTestNode()

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		return kvstore.RequestResult{
			Applied: true,
			Value:   "\xff",
		}, true, nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Put(
		context.Background(),
		newKVPutRequest(),
	)

	if err != nil ||
		response == nil ||
		response.Applied ||
		response.Value != "" {
		t.Fatalf("response=%v err=%v", response, err)
	}

	requireKVValidationCode(
		t,
		response.Error,
		shardraftpb.ErrorCode_ERROR_CODE_INTERNAL,
	)
}
