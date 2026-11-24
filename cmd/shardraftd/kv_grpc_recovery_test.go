package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

type kvRecoveryFixture struct {
	t      *testing.T
	path   string
	node   *shard.Shard
	server *kvGRPCServer
}

func newKVRecoveryFixture(t *testing.T) *kvRecoveryFixture {
	t.Helper()

	f := &kvRecoveryFixture{
		t:    t,
		path: filepath.Join(t.TempDir(), "node.wal"),
	}

	t.Cleanup(func() {
		if f.node != nil {
			if err := f.node.Close(); err != nil {
				t.Error(err)
			}
			f.node = nil
		}
	})

	f.open()

	return f
}

func (f *kvRecoveryFixture) open() {
	f.t.Helper()

	if f.node != nil {
		f.t.Fatal("cannot reopen an active shard")
	}

	node, err := shard.Open(shard.Config{
		ID:              1,
		ElectionTimeout: 10,
		WALPath:         f.path,
		Send: func([]raft.Message) error {
			return nil
		},
	})

	if err != nil {
		f.t.Fatal(err)
	}

	f.node = node
	f.server = newKVGRPCServer(1, node, nil)

	for i := 0; i < 100; i++ {
		if err := node.Tick(); err != nil {
			f.t.Fatal(err)
		}

		st, applied, err := node.StatusSnapshot()

		if err != nil {
			f.t.Fatal(err)
		}

		if st.Role == raft.Leader &&
			st.Commit > 0 &&
			applied == st.Commit {
			return
		}
	}

	f.t.Fatal("recovered shard did not become ready")
}

func (f *kvRecoveryFixture) restart() {
	f.t.Helper()

	before, _, err := f.node.StatusSnapshot()
	if err != nil {
		f.t.Fatal(err)
	}

	if err := f.node.Close(); err != nil {
		f.t.Fatal(err)
	}

	f.node = nil
	f.server = nil

	f.open()

	after, applied, err := f.node.StatusSnapshot()
	if err != nil {
		f.t.Fatal(err)
	}

	if after.Term <= before.Term ||
		after.Commit < before.Commit ||
		applied != after.Commit {
		f.t.Fatalf(
			"invalid recovery: before=%+v after=%+v applied=%d",
			before, after, applied,
		)
	}

	f.t.Logf(
		"restart: term=%d->%d commit=%d->%d applied=%d",
		before.Term, after.Term,
		before.Commit, after.Commit,
		applied,
	)
}

func (f *kvRecoveryFixture) mutate(
	command kvstore.Command,
) *shardraftpb.MutationResponse {
	f.t.Helper()

	return invokeKVDedupMutation(
		f.t, f.server, command,
	)
}

func (f *kvRecoveryFixture) requireResult(
	command kvstore.Command,
	applied bool,
	value string,
) {
	f.t.Helper()

	requireKVDedupResult(
		f.t, f.mutate(command), applied, value,
	)
}

func (f *kvRecoveryFixture) requireValue(
	key, value string,
	found bool,
) {
	f.t.Helper()

	requireKVDedupValue(
		f.t, f.node, key, value, found,
	)
}

func (f *kvRecoveryFixture) commit() uint64 {
	f.t.Helper()

	return kvDedupCommit(f.t, f.node)
}

func (f *kvRecoveryFixture) requireUnchangedCommit(
	before uint64,
) {
	f.t.Helper()

	if after := f.commit(); after != before {
		f.t.Fatalf(
			"retry advanced commit: before=%d after=%d",
			before, after,
		)
	}
}

func TestKVGRPCRecoveryPutResult(t *testing.T) {
	f := newKVRecoveryFixture(t)

	original := kvDedupPut("original", "alpha", "100")

	f.requireResult(original, true, "100")

	f.requireResult(
		kvDedupPut("overwrite", "alpha", "200"),
		true, "200",
	)

	f.restart()

	f.requireValue("alpha", "200", true)

	before := f.commit()

	f.requireResult(original, true, "100")
	f.requireValue("alpha", "200", true)
	f.requireUnchangedCommit(before)
}

func TestKVGRPCRecoveryDeleteResult(t *testing.T) {
	for _, existed := range []bool{true, false} {
		t.Run(fmt.Sprintf("Existing=%v", existed), func(t *testing.T) {
			f := newKVRecoveryFixture(t)

			if existed {
				f.requireResult(
					kvDedupPut("seed", "alpha", "100"),
					true, "100",
				)
			}

			command := kvDedupPut("delete", "alpha", "")
			command.Op = kvstore.OpDelete

			f.requireResult(command, existed, "")
			f.requireValue("alpha", "", false)

			f.restart()

			f.requireValue("alpha", "", false)

			f.requireResult(
				kvDedupPut("recreate", "alpha", "300"),
				true, "300",
			)

			f.restart()

			before := f.commit()

			f.requireResult(command, existed, "")
			f.requireValue("alpha", "300", true)
			f.requireUnchangedCommit(before)
		})
	}
}

func TestKVGRPCRecoveryCASResult(t *testing.T) {
	for _, matched := range []bool{true, false} {
		t.Run(fmt.Sprintf("Matched=%v", matched), func(t *testing.T) {
			f := newKVRecoveryFixture(t)

			f.requireResult(
				kvDedupPut("seed", "alpha", "100"),
				true, "100",
			)

			command := kvDedupPut("cas", "alpha", "200")
			command.Op = kvstore.OpCompareAndSwap
			command.Expected = "999"

			resultValue := ""
			storedValue := "100"

			if matched {
				command.Expected = "100"
				resultValue = "200"
				storedValue = "200"
			}

			f.requireResult(command, matched, resultValue)
			f.requireValue("alpha", storedValue, true)

			f.restart()

			f.requireValue("alpha", storedValue, true)

			// Make the comparison valid again. Retrying the
			// original identity must not execute it again.
			f.requireResult(
				kvDedupPut(
					"reset", "alpha", command.Expected,
				),
				true, command.Expected,
			)

			f.restart()

			before := f.commit()

			f.requireResult(command, matched, resultValue)

			f.requireValue(
				"alpha", command.Expected, true,
			)

			f.requireUnchangedCommit(before)
		})
	}
}

func TestKVGRPCRecoveryConflict(t *testing.T) {
	f := newKVRecoveryFixture(t)

	f.requireResult(
		kvDedupPut("seed", "alpha", "100"),
		true, "100",
	)

	original := kvDedupPut("shared", "alpha", "200")
	original.Op = kvstore.OpCompareAndSwap
	original.Expected = "100"

	f.requireResult(original, true, "200")

	f.restart()

	before := f.commit()

	changedKey := original
	changedKey.Key = "beta"

	changedValue := original
	changedValue.Value = "300"

	changedExpected := original
	changedExpected.Expected = "999"

	put := kvDedupPut("shared", "alpha", "400")

	del := kvDedupPut("shared", "alpha", "")
	del.Op = kvstore.OpDelete

	for _, command := range []kvstore.Command{
		changedKey,
		changedValue,
		changedExpected,
		put,
		del,
	} {
		response := f.mutate(command)

		requireKVValidationCode(
			t, response.Error,
			shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
		)

		if response.Applied || response.Value != "" {
			t.Fatalf("invalid conflict response: %v", response)
		}

		f.requireUnchangedCommit(before)
	}

	f.requireResult(original, true, "200")
	f.requireValue("alpha", "200", true)
	f.requireValue("beta", "", false)
	f.requireUnchangedCommit(before)
}

func TestKVGRPCRecoveryUnknownOutcome(t *testing.T) {
	for _, operation := range []kvstore.Operation{
		kvstore.OpPut,
		kvstore.OpDelete,
		kvstore.OpCompareAndSwap,
	} {
		t.Run(string(operation), func(t *testing.T) {
			f := newKVRecoveryFixture(t)

			f.requireResult(
				kvDedupPut("seed", "alpha", "100"),
				true, "100",
			)

			command := kvDedupPut(
				"uncertain", "alpha", "200",
			)
			command.Op = operation

			switch operation {
			case kvstore.OpDelete:
				command.Value = ""
			case kvstore.OpCompareAndSwap:
				command.Expected = "100"
			}

			fault := &kvErrorAfterProposalNode{
				kvGRPCNode: f.node,
			}

			faultServer := newKVGRPCServer(1, fault, nil)

			before := f.commit()

			response := invokeKVDedupMutation(
				t, faultServer, command,
			)

			requireKVValidationCode(
				t, response.Error,
				shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
			)

			if response.Applied || response.Value != "" {
				t.Fatalf(
					"unknown outcome reported success: %v",
					response,
				)
			}

			if f.commit() != before+1 {
				t.Fatal("mutation did not commit exactly once")
			}

			f.restart()

			recoveredCommit := f.commit()

			f.requireResult(command, true, command.Value)

			f.requireValue(
				"alpha", command.Value,
				operation != kvstore.OpDelete,
			)

			f.requireUnchangedCommit(recoveredCommit)

			t.Logf(
				"operation=%s recovered=true duplicate_commit=false",
				operation,
			)
		})
	}
}

func TestKVGRPCRecoveryRepeatedRestarts(t *testing.T) {
	f := newKVRecoveryFixture(t)

	original := kvDedupPut("original", "alpha", "100")

	f.requireResult(original, true, "100")

	for i := 1; i <= 5; i++ {
		value := fmt.Sprintf("value-%d", i)

		f.requireResult(
			kvDedupPut(
				fmt.Sprintf("overwrite-%d", i),
				"alpha", value,
			),
			true, value,
		)

		f.restart()

		before := f.commit()

		f.requireResult(original, true, "100")
		f.requireValue("alpha", value, true)
		f.requireUnchangedCommit(before)
	}
}

func TestKVGRPCRecoveryIndependentClients(t *testing.T) {
	f := newKVRecoveryFixture(t)

	first := kvDedupPut("shared", "alpha", "100")
	second := kvDedupPut("shared", "alpha", "200")
	second.ClientID = "another-client"

	f.requireResult(first, true, "100")
	f.requireResult(second, true, "200")

	f.restart()

	before := f.commit()

	f.requireResult(first, true, "100")
	f.requireResult(second, true, "200")
	f.requireValue("alpha", "200", true)
	f.requireUnchangedCommit(before)
}

func TestKVGRPCRecoveryNewRequests(t *testing.T) {
	f := newKVRecoveryFixture(t)

	f.requireResult(
		kvDedupPut("seed", "alpha", "100"),
		true, "100",
	)

	f.restart()

	before := f.commit()

	put := kvDedupPut("new-put", "alpha", "200")

	f.requireResult(put, true, "200")

	cas := kvDedupPut("new-cas", "alpha", "300")
	cas.Op = kvstore.OpCompareAndSwap
	cas.Expected = "200"

	f.requireResult(cas, true, "300")

	del := kvDedupPut("new-delete", "alpha", "")
	del.Op = kvstore.OpDelete

	f.requireResult(del, true, "")
	f.requireValue("alpha", "", false)

	if f.commit() != before+3 {
		t.Fatal("new mutations did not commit exactly once each")
	}

	f.restart()

	recoveredCommit := f.commit()

	f.requireResult(put, true, "200")
	f.requireResult(cas, true, "300")
	f.requireResult(del, true, "")

	f.requireValue("alpha", "", false)
	f.requireUnchangedCommit(recoveredCommit)
}
