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

func newKVDeleteRequest() *shardraftpb.DeleteRequest {
	return &shardraftpb.DeleteRequest{
		Header: &shardraftpb.RequestHeader{
			ShardId:   1,
			Epoch:     1,
			ClientId:  "client-1",
			RequestId: "delete-1",
		},
		Key: "alpha",
	}
}

func TestKVGRPCDeleteCompleted(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		applied bool
	}{
		{"ExistingKey", "alpha", true},
		{"MissingKey", "missing", false},
		{"UnicodeKey", "hello世界", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newKVMutationTestNode()

			request := newKVDeleteRequest()
			request.Key = tc.key

			proposals := 0
			lookups := 0

			verify := func(command kvstore.Command) {
				t.Helper()

				if command.Op != kvstore.OpDelete ||
					command.Key != request.Key ||
					command.ClientID != request.Header.ClientId ||
					command.RequestID != request.Header.RequestId ||
					command.Value != "" ||
					command.Expected != "" {
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
					Applied: tc.applied,
				}, true, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Delete(
				context.Background(),
				request,
			)

			if err != nil ||
				response == nil ||
				response.Error != nil ||
				response.Applied != tc.applied ||
				response.Value != "" ||
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

func TestKVGRPCDeleteDuplicate(t *testing.T) {
	tests := []struct {
		name    string
		applied bool
	}{
		{"PreviouslyDeleted", true},
		{"PreviouslyMissing", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newKVMutationTestNode()

			node.checkedResult = func(
				command kvstore.Command,
			) (kvstore.RequestResult, bool, error) {
				if command.Op != kvstore.OpDelete {
					t.Fatalf("incorrect command: %+v", command)
				}

				return kvstore.RequestResult{
					Applied: tc.applied,
				}, true, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Delete(
				context.Background(),
				newKVDeleteRequest(),
			)

			if err != nil ||
				response == nil ||
				response.Error != nil ||
				response.Applied != tc.applied {
				t.Fatalf("response=%v err=%v", response, err)
			}
		})
	}
}

func TestKVGRPCDeletePending(t *testing.T) {
	node := newKVMutationTestNode()

	proposals := 0
	lookups := 0

	node.propose = func(command kvstore.Command) error {
		proposals++
		return shard.ErrRequestPending
	}

	node.checkedResult = func(
		command kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		lookups++

		if lookups < 3 {
			return kvstore.RequestResult{}, false, nil
		}

		return kvstore.RequestResult{
			Applied: true,
		}, true, nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Delete(
		context.Background(),
		newKVDeleteRequest(),
	)

	if err != nil ||
		response == nil ||
		response.Error != nil ||
		!response.Applied ||
		proposals != 1 ||
		lookups < 3 {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCDeleteInvalidRequests(t *testing.T) {
	invalid := shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST
	wrong := shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD

	tests := []struct {
		name     string
		change   func(*shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest
		expected shardraftpb.ErrorCode
	}{
		{"Nil", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			return nil
		}, invalid},
		{"MissingHeader", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Header = nil
			return r
		}, invalid},
		{"ZeroShard", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Header.ShardId = 0
			return r
		}, invalid},
		{"WrongShard", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Header.ShardId = 2
			return r
		}, wrong},
		{"WrongEpoch", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Header.Epoch = 2
			return r
		}, wrong},
		{"EmptyKey", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Key = ""
			return r
		}, invalid},
		{"MissingClient", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Header.ClientId = ""
			return r
		}, invalid},
		{"MissingRequest", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Header.RequestId = ""
			return r
		}, invalid},
		{"MissingBothIDs", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Header.ClientId = ""
			r.Header.RequestId = ""
			return r
		}, invalid},
		{"InvalidKeyUTF8", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Key = "\xff"
			return r
		}, invalid},
		{"InvalidClientUTF8", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Header.ClientId = "\xff"
			return r
		}, invalid},
		{"Oversized", func(r *shardraftpb.DeleteRequest) *shardraftpb.DeleteRequest {
			r.Key = strings.Repeat("a", kvMaxRequestSize+1)
			return r
		}, invalid},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newKVGRPCServer(
				1,
				newKVMutationTestNode(),
				nil,
			)

			response, err := server.Delete(
				context.Background(),
				tc.change(newKVDeleteRequest()),
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
				tc.expected,
			)
		})
	}
}

func TestKVGRPCDeleteNonLeader(t *testing.T) {
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

		response, err := server.Delete(
			context.Background(),
			newKVDeleteRequest(),
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

func TestKVGRPCDeleteUnavailable(t *testing.T) {
	node := newKVMutationTestNode()
	node.statusErr = shard.ErrClosed

	for _, backend := range []kvGRPCNode{nil, node} {
		server := newKVGRPCServer(1, backend, nil)

		response, err := server.Delete(
			context.Background(),
			newKVDeleteRequest(),
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

func TestKVGRPCDeleteConflict(t *testing.T) {
	for _, stage := range []string{
		"AppliedLookup",
		"Proposal",
		"Confirmation",
	} {
		t.Run(stage, func(t *testing.T) {
			node := newKVMutationTestNode()

			proposals := 0

			node.propose = func(kvstore.Command) error {
				proposals++

				if stage == "Proposal" {
					return kvstore.ErrRequestConflict
				}

				return nil
			}

			node.checkedResult = func(
				kvstore.Command,
			) (kvstore.RequestResult, bool, error) {
				if stage == "AppliedLookup" ||
					(stage == "Confirmation" && proposals > 0) {
					return kvstore.RequestResult{},
						false,
						kvstore.ErrRequestConflict
				}

				return kvstore.RequestResult{}, false, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Delete(
				context.Background(),
				newKVDeleteRequest(),
			)

			if err != nil ||
				response == nil ||
				response.Applied {
				t.Fatalf("response=%v err=%v", response, err)
			}

			requireKVValidationCode(
				t,
				response.Error,
				shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
			)

			if stage == "AppliedLookup" && proposals != 0 {
				t.Fatal("conflicting request was proposed")
			}
		})
	}
}

func TestKVGRPCDeleteUnknownOutcome(t *testing.T) {
	for _, stage := range []string{
		"Proposal",
		"Confirmation",
	} {
		t.Run(stage, func(t *testing.T) {
			node := newKVMutationTestNode()

			proposals := 0

			node.propose = func(kvstore.Command) error {
				proposals++

				if stage == "Proposal" {
					return errors.New("private WAL error")
				}

				return nil
			}

			node.checkedResult = func(
				kvstore.Command,
			) (kvstore.RequestResult, bool, error) {
				if stage == "Confirmation" && proposals > 0 {
					return kvstore.RequestResult{},
						false,
						shard.ErrStopped
				}

				return kvstore.RequestResult{}, false, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Delete(
				context.Background(),
				newKVDeleteRequest(),
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

			if strings.Contains(response.Error.Message, "private") {
				t.Fatal("internal error leaked")
			}
		})
	}
}

func TestKVGRPCDeleteCanceledBeforeSubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server := newKVGRPCServer(
		1,
		newKVMutationTestNode(),
		nil,
	)

	response, err := server.Delete(
		ctx,
		newKVDeleteRequest(),
	)

	if response != nil ||
		status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCDeleteCanceledAfterSubmission(t *testing.T) {
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

	response, err := server.Delete(
		ctx,
		newKVDeleteRequest(),
	)

	if response != nil ||
		status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCDeleteDeadline(t *testing.T) {
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

	response, err := server.Delete(
		ctx,
		newKVDeleteRequest(),
	)

	if response != nil ||
		status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCDeleteCommitTimeout(t *testing.T) {
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

	response, err := server.Delete(
		context.Background(),
		newKVDeleteRequest(),
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

func TestKVGRPCDeletePreservesOriginalResult(t *testing.T) {
	node := newKVMutationTestNode()

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		return kvstore.RequestResult{
			Applied: true,
			Value:   "original-result",
		}, true, nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Delete(
		context.Background(),
		newKVDeleteRequest(),
	)

	if err != nil ||
		response == nil ||
		response.Error != nil ||
		!response.Applied ||
		response.Value != "original-result" {
		t.Fatalf("response=%v err=%v", response, err)
	}
}
