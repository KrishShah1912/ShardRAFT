package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvErrorStatusNode struct {
	kvGRPCNode
	onStatus func()
}

func (n *kvErrorStatusNode) StatusSnapshot() (
	raft.Status, uint64, error,
) {
	n.onStatus()
	return n.kvGRPCNode.StatusSnapshot()
}

func TestKVGRPCErrorsCanceledDuringStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend := newKVMutationTestNode()

	node := &kvErrorStatusNode{
		kvGRPCNode: backend,
		onStatus:   cancel,
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Put(ctx, newKVPutRequest())

	if response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCErrorsCanceledDuringConfirmation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := newKVMutationTestNode()

	proposals := 0
	lookups := 0

	node.propose = func(kvstore.Command) error {
		proposals++
		return nil
	}

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		lookups++

		if proposals > 0 {
			cancel()

			return kvstore.RequestResult{
				Applied: true,
				Value:   "100",
			}, true, nil
		}

		return kvstore.RequestResult{}, false, nil
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Put(ctx, newKVPutRequest())

	if response != nil ||
		status.Code(err) != codes.Canceled ||
		proposals != 1 ||
		lookups < 2 {
		t.Fatalf(
			"response=%v err=%v proposals=%d lookups=%d",
			response, err, proposals, lookups,
		)
	}
}

func TestKVGRPCErrorsCanceledDuringMissingReadStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend := newKVMutationTestNode()

	backend.localRead = func(
		string,
	) (string, bool, error) {
		return "", false, nil
	}

	node := &kvErrorStatusNode{
		kvGRPCNode: backend,
		onStatus:   cancel,
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Get(
		ctx,
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LOCAL,
		),
	)

	if response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCErrorsCanceledDuringFailedReadStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend := newKVMutationTestNode()

	backend.localRead = func(
		string,
	) (string, bool, error) {
		return "", false, shard.ErrStopped
	}

	node := &kvErrorStatusNode{
		kvGRPCNode: backend,
		onStatus:   cancel,
	}

	server := newKVGRPCServer(1, node, nil)

	response, err := server.Get(
		ctx,
		newKVGetRequest(
			shardraftpb.ReadMode_READ_MODE_LOCAL,
		),
	)

	if response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestKVGRPCErrorsWrappedConflict(t *testing.T) {
	for _, stage := range []string{
		"Lookup",
		"Proposal",
		"Confirmation",
	} {
		t.Run(stage, func(t *testing.T) {
			node := newKVMutationTestNode()

			proposals := 0

			wrapped := fmt.Errorf(
				"private storage detail: %w",
				kvstore.ErrRequestConflict,
			)

			node.propose = func(kvstore.Command) error {
				proposals++

				if stage == "Proposal" {
					return wrapped
				}

				return nil
			}

			node.checkedResult = func(
				kvstore.Command,
			) (kvstore.RequestResult, bool, error) {
				if stage == "Lookup" ||
					(stage == "Confirmation" && proposals > 0) {
					return kvstore.RequestResult{}, false, wrapped
				}

				return kvstore.RequestResult{}, false, nil
			}

			server := newKVGRPCServer(1, node, nil)

			response, err := server.Put(
				context.Background(),
				newKVPutRequest(),
			)

			if err != nil || response == nil || response.Applied {
				t.Fatalf("response=%v err=%v", response, err)
			}

			requireKVValidationCode(
				t, response.Error,
				shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
			)

			if strings.Contains(response.Error.Message, "private") {
				t.Fatal("private storage detail leaked")
			}

			if stage == "Lookup" && proposals != 0 {
				t.Fatal("conflicting command was proposed")
			}
		})
	}
}

func TestKVGRPCErrorsWrappedNotLeader(t *testing.T) {
	node := newKVMutationTestNode()

	node.checkedResult = func(
		kvstore.Command,
	) (kvstore.RequestResult, bool, error) {
		return kvstore.RequestResult{}, false, nil
	}

	node.propose = func(kvstore.Command) error {
		node.nodeStatus.Role = raft.Follower
		node.nodeStatus.LeaderID = 2
		node.nodeStatus.Term = 4

		return fmt.Errorf(
			"private proposal detail: %w",
			raft.ErrNotLeader,
		)
	}

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
		t, response.Error,
		shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
	)

	if response.Error.LeaderId != 2 ||
		response.Error.LeaderAddress != "127.0.0.1:19092" ||
		response.Error.Term != 4 {
		t.Fatalf("incorrect routing metadata: %v", response.Error)
	}

	if strings.Contains(response.Error.Message, "private") {
		t.Fatal("private proposal detail leaked")
	}
}

func TestKVGRPCErrorsInternalErrorRedaction(t *testing.T) {
	for _, stage := range []string{
		"Lookup",
		"Proposal",
		"Confirmation",
	} {
		t.Run(stage, func(t *testing.T) {
			node := newKVMutationTestNode()

			proposals := 0

			privateError := errors.New(
				"private WAL path /tmp/internal/node.wal",
			)

			node.propose = func(kvstore.Command) error {
				proposals++

				if stage == "Proposal" {
					return privateError
				}

				return nil
			}

			node.checkedResult = func(
				kvstore.Command,
			) (kvstore.RequestResult, bool, error) {
				if stage == "Lookup" ||
					(stage == "Confirmation" && proposals > 0) {
					return kvstore.RequestResult{}, false, privateError
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

			expected := shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN

			if stage == "Lookup" {
				expected = shardraftpb.ErrorCode_ERROR_CODE_INTERNAL
			}

			requireKVValidationCode(
				t, response.Error, expected,
			)

			for _, forbidden := range []string{
				"private",
				"/tmp/internal",
				"node.wal",
			} {
				if strings.Contains(response.Error.Message, forbidden) {
					t.Fatalf("internal information leaked: %v", response.Error)
				}
			}

			if stage == "Lookup" && proposals != 0 {
				t.Fatal("request was proposed after lookup failure")
			}
		})
	}
}

// Simulate a response failure after the real shard has
// successfully accepted and applied a mutation.
type kvErrorAfterProposalNode struct {
	kvGRPCNode
	proposals int
}

func (n *kvErrorAfterProposalNode) Propose(
	command kvstore.Command,
) error {
	n.proposals++

	if err := n.kvGRPCNode.Propose(command); err != nil {
		return err
	}

	return errors.New("simulated post-proposal failure")
}

func TestKVGRPCErrorsUnknownOutcomeRetry(t *testing.T) {
	for _, operation := range []kvstore.Operation{
		kvstore.OpPut,
		kvstore.OpDelete,
		kvstore.OpCompareAndSwap,
	} {
		t.Run(string(operation), func(t *testing.T) {
			node, normalServer := newKVDedupShard(t)

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, normalServer,
					kvDedupPut("seed", "alpha", "100")),
				true, "100",
			)

			command := kvDedupPut("uncertain", "alpha", "200")
			command.Op = operation

			switch operation {
			case kvstore.OpDelete:
				command.Value = ""
			case kvstore.OpCompareAndSwap:
				command.Expected = "100"
			}

			fault := &kvErrorAfterProposalNode{
				kvGRPCNode: node,
			}

			faultServer := newKVGRPCServer(1, fault, nil)

			before := kvDedupCommit(t, node)

			response := invokeKVDedupMutation(
				t, faultServer, command,
			)

			requireKVValidationCode(
				t, response.Error,
				shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
			)

			if response.Applied || response.Value != "" {
				t.Fatalf("uncertain outcome reported success: %v", response)
			}

			after := kvDedupCommit(t, node)

			if after != before+1 {
				t.Fatal("real mutation did not commit exactly once")
			}

			requireKVDedupValue(
				t, node, "alpha", command.Value,
				operation != kvstore.OpDelete,
			)

			// Retry the same identity and exact command.
			retry := invokeKVDedupMutation(
				t, faultServer, command,
			)

			requireKVDedupResult(
				t, retry, true, command.Value,
			)

			if fault.proposals != 1 {
				t.Fatalf("retry proposed again: %d", fault.proposals)
			}

			if kvDedupCommit(t, node) != after {
				t.Fatal("retry advanced commit index")
			}

			t.Logf(
				"operation=%s proposals=%d commit_delta=%d retry_confirmed=true",
				operation, fault.proposals, after-before,
			)
		})
	}
}
