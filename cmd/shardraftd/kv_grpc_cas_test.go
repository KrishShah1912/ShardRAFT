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

func newKVCASRequest() *shardraftpb.CompareAndSwapRequest {
	return &shardraftpb.CompareAndSwapRequest{
		Header: &shardraftpb.RequestHeader{
			ShardId:   1,
			Epoch:     1,
			ClientId:  "client-1",
			RequestId: "cas-1",
		},
		Key:      "alpha",
		Expected: "100",
		Value:    "200",
	}
}

func TestKVGRPCCASCompleted(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		value    string
		applied  bool
	}{
		{"MatchingValue", "100", "200", true},
		{"MismatchedValue", "999", "200", false},
		{"MissingKey", "100", "200", false},
		{"EmptyExpected", "", "200", true},
		{"EmptyReplacement", "100", "", true},
		{"BothEmpty", "", "", true},
		{"UnicodeValues", "世界", "こんにちは", true},
		{"SameValue", "100", "100", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newKVMutationTestNode()

			request := newKVCASRequest()
			request.Expected = tc.expected
			request.Value = tc.value

			if tc.name == "MissingKey" {
				request.Key = "missing"
			}

			proposals := 0
			lookups := 0

			verify := func(command kvstore.Command) {
				t.Helper()

				if command.Op != kvstore.OpCompareAndSwap ||
					command.Key != request.Key ||
					command.Expected != request.Expected ||
					command.Value != request.Value ||
					command.ClientID != request.Header.ClientId ||
					command.RequestID != request.Header.RequestId {
					t.Fatalf("incorrect command: %+v", command)
				}
			}

			resultValue := ""
			if tc.applied {
				resultValue = tc.value
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
					Value:   resultValue,
				}, true, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.CompareAndSwap(
				context.Background(),
				request,
			)

			if err != nil ||
				response == nil ||
				response.Error != nil ||
				response.Applied != tc.applied ||
				response.Value != resultValue ||
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

func TestKVGRPCCASDuplicate(t *testing.T) {
	tests := []struct {
		name    string
		applied bool
		value   string
	}{
		{"OriginalSuccess", true, "200"},
		{"OriginalMismatch", false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newKVMutationTestNode()

			node.checkedResult = func(
				command kvstore.Command,
			) (kvstore.RequestResult, bool, error) {
				if command.Op != kvstore.OpCompareAndSwap {
					t.Fatalf("incorrect command: %+v", command)
				}

				return kvstore.RequestResult{
					Applied: tc.applied,
					Value:   tc.value,
				}, true, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.CompareAndSwap(
				context.Background(),
				newKVCASRequest(),
			)

			if err != nil ||
				response == nil ||
				response.Error != nil ||
				response.Applied != tc.applied ||
				response.Value != tc.value {
				t.Fatalf("response=%v err=%v", response, err)
			}
		})
	}
}

func TestKVGRPCCASPending(t *testing.T) {
	for _, applied := range []bool{true, false} {
		node := newKVMutationTestNode()

		proposals := 0
		lookups := 0

		node.propose = func(kvstore.Command) error {
			proposals++
			return shard.ErrRequestPending
		}

		value := ""
		if applied {
			value = "200"
		}

		node.checkedResult = func(
			kvstore.Command,
		) (kvstore.RequestResult, bool, error) {
			lookups++

			if lookups < 3 {
				return kvstore.RequestResult{}, false, nil
			}

			return kvstore.RequestResult{
				Applied: applied,
				Value:   value,
			}, true, nil
		}

		server := newKVGRPCServer(1, node, nil)

		response, err := server.CompareAndSwap(
			context.Background(),
			newKVCASRequest(),
		)

		if err != nil ||
			response == nil ||
			response.Error != nil ||
			response.Applied != applied ||
			response.Value != value ||
			proposals != 1 ||
			lookups < 3 {
			t.Fatalf("response=%v err=%v", response, err)
		}
	}
}

func TestKVGRPCCASInvalidRequests(t *testing.T) {
	invalid := shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST
	wrong := shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD

	tests := []struct {
		name   string
		change func(
			*shardraftpb.CompareAndSwapRequest,
		) *shardraftpb.CompareAndSwapRequest
		expected shardraftpb.ErrorCode
	}{
		{"Nil", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			return nil
		}, invalid},
		{"MissingHeader", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Header = nil
			return r
		}, invalid},
		{"ZeroShard", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Header.ShardId = 0
			return r
		}, invalid},
		{"WrongShard", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Header.ShardId = 2
			return r
		}, wrong},
		{"WrongEpoch", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Header.Epoch = 2
			return r
		}, wrong},
		{"EmptyKey", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Key = ""
			return r
		}, invalid},
		{"MissingClient", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Header.ClientId = ""
			return r
		}, invalid},
		{"MissingRequest", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Header.RequestId = ""
			return r
		}, invalid},
		{"MissingBothIDs", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Header.ClientId = ""
			r.Header.RequestId = ""
			return r
		}, invalid},
		{"InvalidKey", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Key = "\xff"
			return r
		}, invalid},
		{"InvalidExpected", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Expected = "\xff"
			return r
		}, invalid},
		{"InvalidReplacement", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Value = "\xff"
			return r
		}, invalid},
		{"OversizedExpected", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Expected = strings.Repeat("a", kvMaxRequestSize+1)
			return r
		}, invalid},
		{"OversizedReplacement", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Value = strings.Repeat("a", kvMaxRequestSize+1)
			return r
		}, invalid},
		{"OversizedCombined", func(r *shardraftpb.CompareAndSwapRequest) *shardraftpb.CompareAndSwapRequest {
			r.Expected = strings.Repeat("a", kvMaxRequestSize/2)
			r.Value = strings.Repeat("b", kvMaxRequestSize/2)
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

			response, err := server.CompareAndSwap(
				context.Background(),
				tc.change(newKVCASRequest()),
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

func TestKVGRPCCASNonLeader(t *testing.T) {
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

		response, err := server.CompareAndSwap(
			context.Background(),
			newKVCASRequest(),
		)

		if err != nil ||
			response == nil ||
			response.Applied {
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

func TestKVGRPCCASUnavailable(t *testing.T) {
	node := newKVMutationTestNode()
	node.statusErr = shard.ErrClosed

	for _, backend := range []kvGRPCNode{nil, node} {
		server := newKVGRPCServer(1, backend, nil)

		response, err := server.CompareAndSwap(
			context.Background(),
			newKVCASRequest(),
		)

		if err != nil ||
			response == nil ||
			response.Applied {
			t.Fatalf("response=%v err=%v", response, err)
		}

		requireKVValidationCode(
			t,
			response.Error,
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
		)
	}
}

func TestKVGRPCCASConflict(t *testing.T) {
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

			response, err := server.CompareAndSwap(
				context.Background(),
				newKVCASRequest(),
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

func TestKVGRPCCASUnknownOutcome(t *testing.T) {
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

			response, err := server.CompareAndSwap(
				context.Background(),
				newKVCASRequest(),
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

func TestKVGRPCCASCanceledBeforeSubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server := newKVGRPCServer(
		1,
		newKVMutationTestNode(),
		nil,
	)

	response, err := server.CompareAndSwap(
		ctx,
		newKVCASRequest(),
	)

	if response != nil ||
		status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCCASCanceledAfterSubmission(t *testing.T) {
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

	response, err := server.CompareAndSwap(
		ctx,
		newKVCASRequest(),
	)

	if response != nil ||
		status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCCASDeadline(t *testing.T) {
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

	response, err := server.CompareAndSwap(
		ctx,
		newKVCASRequest(),
	)

	if response != nil ||
		status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCCASCommitTimeout(t *testing.T) {
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

	response, err := server.CompareAndSwap(
		context.Background(),
		newKVCASRequest(),
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

func TestKVGRPCCASInvalidStoredResult(t *testing.T) {
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

	response, err := server.CompareAndSwap(
		context.Background(),
		newKVCASRequest(),
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
