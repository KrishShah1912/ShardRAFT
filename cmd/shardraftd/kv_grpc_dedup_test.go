package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

func newKVDedupShard(t *testing.T) (*shard.Shard, *kvGRPCServer) {
	t.Helper()

	node, err := shard.Open(shard.Config{
		ID:              1,
		ElectionTimeout: 10,
		WALPath:         filepath.Join(t.TempDir(), "node.wal"),
		Send: func([]raft.Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})

	for i := 0; i < 100; i++ {
		if err := node.Tick(); err != nil {
			t.Fatal(err)
		}

		st, applied, err := node.StatusSnapshot()
		if err != nil {
			t.Fatal(err)
		}

		if st.Role == raft.Leader &&
			st.Commit > 0 &&
			applied == st.Commit {
			return node, newKVGRPCServer(1, node, nil)
		}
	}

	t.Fatal("single-node shard did not become ready")
	return nil, nil
}

func invokeKVDedupMutation(
	t *testing.T,
	server *kvGRPCServer,
	command kvstore.Command,
) *shardraftpb.MutationResponse {
	t.Helper()

	header := &shardraftpb.RequestHeader{
		ShardId:   1,
		Epoch:     1,
		ClientId:  command.ClientID,
		RequestId: command.RequestID,
	}

	ctx := context.Background()

	var response *shardraftpb.MutationResponse
	var err error

	switch command.Op {
	case kvstore.OpPut:
		response, err = server.Put(ctx, &shardraftpb.PutRequest{
			Header: header,
			Key:    command.Key,
			Value:  command.Value,
		})

	case kvstore.OpDelete:
		response, err = server.Delete(ctx, &shardraftpb.DeleteRequest{
			Header: header,
			Key:    command.Key,
		})

	case kvstore.OpCompareAndSwap:
		response, err = server.CompareAndSwap(
			ctx,
			&shardraftpb.CompareAndSwapRequest{
				Header:   header,
				Key:      command.Key,
				Expected: command.Expected,
				Value:    command.Value,
			},
		)

	default:
		t.Fatalf("unsupported operation: %v", command.Op)
	}

	if err != nil || response == nil {
		t.Fatalf("response=%v err=%v", response, err)
	}

	return response
}

func requireKVDedupResult(
	t *testing.T,
	response *shardraftpb.MutationResponse,
	applied bool,
	value string,
) {
	t.Helper()

	if response.Error != nil ||
		response.Applied != applied ||
		response.Value != value {
		t.Fatalf(
			"response=%v expected applied=%v value=%q",
			response, applied, value,
		)
	}
}

func requireKVDedupValue(
	t *testing.T,
	node *shard.Shard,
	key string,
	value string,
	found bool,
) {
	t.Helper()

	actual, exists, err := node.Get(key)

	if err != nil || actual != value || exists != found {
		t.Fatalf(
			"key=%q value=%q found=%v err=%v",
			key, actual, exists, err,
		)
	}
}

func kvDedupCommit(
	t *testing.T,
	node *shard.Shard,
) uint64 {
	t.Helper()

	st, applied, err := node.StatusSnapshot()

	if err != nil || st.Commit != applied {
		t.Fatalf(
			"status=%+v applied=%d err=%v",
			st, applied, err,
		)
	}

	return st.Commit
}

func kvDedupPut(requestID, key, value string) kvstore.Command {
	return kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       key,
		Value:     value,
		ClientID:  "dedup-client",
		RequestID: requestID,
	}
}

func TestKVGRPCDedupCrossOperationConflict(t *testing.T) {
	operations := []kvstore.Operation{
		kvstore.OpPut,
		kvstore.OpDelete,
		kvstore.OpCompareAndSwap,
	}

	for _, firstOp := range operations {
		for _, secondOp := range operations {
			if firstOp == secondOp {
				continue
			}

			t.Run(string(firstOp)+"To"+string(secondOp), func(t *testing.T) {
				node, server := newKVDedupShard(t)

				seed := kvDedupPut("seed", "alpha", "100")
				requireKVDedupResult(
					t, invokeKVDedupMutation(t, server, seed),
					true, "100",
				)

				first := kvDedupPut("shared", "alpha", "200")
				first.Op = firstOp

				switch firstOp {
				case kvstore.OpDelete:
					first.Value = ""
				case kvstore.OpCompareAndSwap:
					first.Expected = "100"
				}

				original := invokeKVDedupMutation(t, server, first)
				requireKVDedupResult(t, original, true, first.Value)

				before := kvDedupCommit(t, node)

				second := kvDedupPut("shared", "alpha", "300")
				second.Op = secondOp

				switch secondOp {
				case kvstore.OpDelete:
					second.Value = ""
				case kvstore.OpCompareAndSwap:
					second.Expected = "200"
				}

				response := invokeKVDedupMutation(t, server, second)

				requireKVValidationCode(
					t, response.Error,
					shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
				)

				if response.Applied || response.Value != "" {
					t.Fatalf("unexpected conflict result: %v", response)
				}

				if kvDedupCommit(t, node) != before {
					t.Fatal("conflicting request advanced commit index")
				}

				retry := invokeKVDedupMutation(t, server, first)
				requireKVDedupResult(t, retry, original.Applied, original.Value)

				requireKVDedupValue(
					t, node, "alpha", first.Value,
					firstOp != kvstore.OpDelete,
				)

				if kvDedupCommit(t, node) != before {
					t.Fatal("completed retry advanced commit index")
				}
			})
		}
	}
}

func TestKVGRPCDedupChangedCommandFields(t *testing.T) {
	tests := []struct {
		name   string
		change func(*kvstore.Command)
	}{
		{"Key", func(c *kvstore.Command) {
			c.Key = "beta"
		}},
		{"Value", func(c *kvstore.Command) {
			c.Value = "300"
		}},
		{"Expected", func(c *kvstore.Command) {
			c.Expected = "999"
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node, server := newKVDedupShard(t)

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server,
					kvDedupPut("seed", "alpha", "100")),
				true, "100",
			)

			original := kvDedupPut("cas", "alpha", "200")
			original.Op = kvstore.OpCompareAndSwap
			original.Expected = "100"

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server, original),
				true, "200",
			)

			before := kvDedupCommit(t, node)

			conflicting := original
			tc.change(&conflicting)

			response := invokeKVDedupMutation(t, server, conflicting)

			requireKVValidationCode(
				t, response.Error,
				shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
			)

			requireKVDedupValue(t, node, "alpha", "200", true)
			requireKVDedupValue(t, node, "beta", "", false)

			if kvDedupCommit(t, node) != before {
				t.Fatal("conflicting command advanced commit index")
			}
		})
	}
}

func TestKVGRPCDedupOriginalPutResult(t *testing.T) {
	node, server := newKVDedupShard(t)

	original := kvDedupPut("first", "alpha", "100")

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, original),
		true, "100",
	)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server,
			kvDedupPut("second", "alpha", "200")),
		true, "200",
	)

	before := kvDedupCommit(t, node)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, original),
		true, "100",
	)

	requireKVDedupValue(t, node, "alpha", "200", true)

	if kvDedupCommit(t, node) != before {
		t.Fatal("Put retry appended another committed entry")
	}
}

func TestKVGRPCDedupOriginalDeleteResult(t *testing.T) {
	for _, existed := range []bool{true, false} {
		name := "Missing"
		if existed {
			name = "Existing"
		}

		t.Run(name, func(t *testing.T) {
			node, server := newKVDedupShard(t)

			if existed {
				requireKVDedupResult(t,
					invokeKVDedupMutation(t, server,
						kvDedupPut("seed", "alpha", "100")),
					true, "100",
				)
			}

			command := kvDedupPut("delete", "alpha", "")
			command.Op = kvstore.OpDelete

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server, command),
				existed, "",
			)

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server,
					kvDedupPut("recreate", "alpha", "300")),
				true, "300",
			)

			before := kvDedupCommit(t, node)

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server, command),
				existed, "",
			)

			requireKVDedupValue(t, node, "alpha", "300", true)

			if kvDedupCommit(t, node) != before {
				t.Fatal("Delete retry advanced commit index")
			}
		})
	}
}

func TestKVGRPCDedupOriginalCASResult(t *testing.T) {
	for _, matched := range []bool{true, false} {
		name := "Mismatch"
		if matched {
			name = "Match"
		}

		t.Run(name, func(t *testing.T) {
			node, server := newKVDedupShard(t)

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server,
					kvDedupPut("seed", "alpha", "100")),
				true, "100",
			)

			command := kvDedupPut("cas", "alpha", "200")
			command.Op = kvstore.OpCompareAndSwap
			command.Expected = "999"

			expectedValue := ""
			if matched {
				command.Expected = "100"
				expectedValue = "200"
			}

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server, command),
				matched, expectedValue,
			)

			// Make the original comparison valid again.
			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server,
					kvDedupPut("reset", "alpha", command.Expected)),
				true, command.Expected,
			)

			before := kvDedupCommit(t, node)

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server, command),
				matched, expectedValue,
			)

			requireKVDedupValue(
				t, node, "alpha", command.Expected, true,
			)

			if kvDedupCommit(t, node) != before {
				t.Fatal("CAS retry advanced commit index")
			}
		})
	}
}

func TestKVGRPCDedupIndependentClients(t *testing.T) {
	node, server := newKVDedupShard(t)

	first := kvDedupPut("same-request", "alpha", "100")
	second := kvDedupPut("same-request", "alpha", "200")
	second.ClientID = "another-client"

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, first),
		true, "100",
	)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, second),
		true, "200",
	)

	before := kvDedupCommit(t, node)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, first),
		true, "100",
	)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, second),
		true, "200",
	)

	requireKVDedupValue(t, node, "alpha", "200", true)

	if kvDedupCommit(t, node) != before {
		t.Fatal("independent client retries advanced commit index")
	}
}

func TestKVGRPCDedupIndependentRequestIDs(t *testing.T) {
	node, server := newKVDedupShard(t)

	first := kvDedupPut("request-a", "alpha", "100")
	second := kvDedupPut("request-b", "alpha", "200")

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, first),
		true, "100",
	)

	before := kvDedupCommit(t, node)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, second),
		true, "200",
	)

	if kvDedupCommit(t, node) != before+1 {
		t.Fatal("independent request did not commit exactly once")
	}

	requireKVDedupValue(t, node, "alpha", "200", true)
}

func TestKVGRPCDedupReadDoesNotConsumeIdentity(t *testing.T) {
	node, server := newKVDedupShard(t)

	command := kvDedupPut("shared", "alpha", "100")

	response, err := server.Get(
		context.Background(),
		&shardraftpb.GetRequest{
			Header: &shardraftpb.RequestHeader{
				ShardId:   1,
				Epoch:     1,
				ClientId:  command.ClientID,
				RequestId: command.RequestID,
			},
			Key:      command.Key,
			ReadMode: shardraftpb.ReadMode_READ_MODE_LOCAL,
		},
	)

	if err != nil || response == nil || response.Found {
		t.Fatalf("response=%v err=%v", response, err)
	}

	requireKVValidationCode(
		t, response.Error,
		shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND,
	)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server, command),
		true, "100",
	)

	requireKVDedupValue(t, node, "alpha", "100", true)
}
