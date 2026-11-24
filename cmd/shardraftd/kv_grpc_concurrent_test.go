package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

const kvConcurrentWorkers = 64

func runKVConcurrentMutations(
	t *testing.T,
	server *kvGRPCServer,
	commands []kvstore.Command,
) []*shardraftpb.MutationResponse {
	t.Helper()

	results := make(
		[]*shardraftpb.MutationResponse,
		len(commands),
	)

	start := make(chan struct{})

	var ready sync.WaitGroup
	var finished sync.WaitGroup

	ready.Add(len(commands))
	finished.Add(len(commands))

	for i, command := range commands {
		go func(index int, command kvstore.Command) {
			defer finished.Done()

			ready.Done()
			<-start

			results[index] = invokeKVDedupMutation(
				t, server, command,
			)
		}(i, command)
	}

	ready.Wait()
	close(start)
	finished.Wait()

	for i, result := range results {
		if result == nil {
			t.Fatalf("missing response for worker %d", i)
		}
	}

	return results
}

func repeatKVConcurrentCommand(
	command kvstore.Command,
) []kvstore.Command {
	commands := make(
		[]kvstore.Command,
		kvConcurrentWorkers,
	)

	for i := range commands {
		commands[i] = command
	}

	return commands
}

func TestKVGRPCConcurrentIdenticalPut(t *testing.T) {
	node, server := newKVDedupShard(t)

	command := kvDedupPut("shared-put", "alpha", "100")

	before := kvDedupCommit(t, node)

	results := runKVConcurrentMutations(
		t, server,
		repeatKVConcurrentCommand(command),
	)

	for _, response := range results {
		requireKVDedupResult(t, response, true, "100")
	}

	requireKVDedupValue(t, node, "alpha", "100", true)

	after := kvDedupCommit(t, node)

	if after != before+1 {
		t.Fatalf(
			"expected one committed mutation: before=%d after=%d",
			before, after,
		)
	}

	t.Logf(
		"workers=%d successful=%d commit_delta=%d",
		len(results), len(results), after-before,
	)
}

func TestKVGRPCConcurrentIdenticalDelete(t *testing.T) {
	for _, existed := range []bool{true, false} {
		t.Run(fmt.Sprintf("Existing=%v", existed), func(t *testing.T) {
			node, server := newKVDedupShard(t)

			if existed {
				requireKVDedupResult(t,
					invokeKVDedupMutation(t, server,
						kvDedupPut("seed", "alpha", "100")),
					true, "100",
				)
			}

			command := kvDedupPut("shared-delete", "alpha", "")
			command.Op = kvstore.OpDelete

			before := kvDedupCommit(t, node)

			results := runKVConcurrentMutations(
				t, server,
				repeatKVConcurrentCommand(command),
			)

			for _, response := range results {
				requireKVDedupResult(t, response, existed, "")
			}

			requireKVDedupValue(t, node, "alpha", "", false)

			if kvDedupCommit(t, node) != before+1 {
				t.Fatal("duplicate Delete committed more than once")
			}
		})
	}
}

func TestKVGRPCConcurrentIdenticalCAS(t *testing.T) {
	for _, matched := range []bool{true, false} {
		t.Run(fmt.Sprintf("Matched=%v", matched), func(t *testing.T) {
			node, server := newKVDedupShard(t)

			requireKVDedupResult(t,
				invokeKVDedupMutation(t, server,
					kvDedupPut("seed", "alpha", "100")),
				true, "100",
			)

			command := kvDedupPut("shared-cas", "alpha", "200")
			command.Op = kvstore.OpCompareAndSwap
			command.Expected = "999"

			expectedResult := ""
			expectedStored := "100"

			if matched {
				command.Expected = "100"
				expectedResult = "200"
				expectedStored = "200"
			}

			before := kvDedupCommit(t, node)

			results := runKVConcurrentMutations(
				t, server,
				repeatKVConcurrentCommand(command),
			)

			for _, response := range results {
				requireKVDedupResult(
					t, response, matched, expectedResult,
				)
			}

			requireKVDedupValue(
				t, node, "alpha", expectedStored, true,
			)

			if kvDedupCommit(t, node) != before+1 {
				t.Fatal("duplicate CAS committed more than once")
			}
		})
	}
}

func TestKVGRPCConcurrentConflictingPut(t *testing.T) {
	node, server := newKVDedupShard(t)

	commands := make(
		[]kvstore.Command,
		kvConcurrentWorkers,
	)

	for i := range commands {
		commands[i] = kvDedupPut(
			"conflicting-put",
			"alpha",
			fmt.Sprintf("value-%d", i),
		)
	}

	before := kvDedupCommit(t, node)

	results := runKVConcurrentMutations(
		t, server, commands,
	)

	winners := 0
	conflicts := 0
	winnerValue := ""

	for i, response := range results {
		if response.Error == nil {
			requireKVDedupResult(
				t, response, true, commands[i].Value,
			)

			winners++
			winnerValue = response.Value
			continue
		}

		requireKVValidationCode(
			t, response.Error,
			shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
		)

		if response.Applied || response.Value != "" {
			t.Fatalf("invalid conflict response: %v", response)
		}

		conflicts++
	}

	if winners != 1 || conflicts != len(commands)-1 {
		t.Fatalf(
			"winners=%d conflicts=%d",
			winners, conflicts,
		)
	}

	requireKVDedupValue(t, node, "alpha", winnerValue, true)

	after := kvDedupCommit(t, node)

	if after != before+1 {
		t.Fatal("conflicting requests advanced commit more than once")
	}

	t.Logf(
		"workers=%d winners=%d conflicts=%d commit_delta=%d",
		len(commands), winners, conflicts, after-before,
	)
}

func TestKVGRPCConcurrentCrossOperationConflict(t *testing.T) {
	node, server := newKVDedupShard(t)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server,
			kvDedupPut("seed", "alpha", "100")),
		true, "100",
	)

	put := kvDedupPut("shared-operation", "alpha", "200")

	del := kvDedupPut("shared-operation", "alpha", "")
	del.Op = kvstore.OpDelete

	cas := kvDedupPut("shared-operation", "alpha", "300")
	cas.Op = kvstore.OpCompareAndSwap
	cas.Expected = "100"

	commands := []kvstore.Command{put, del, cas}

	before := kvDedupCommit(t, node)

	results := runKVConcurrentMutations(
		t, server, commands,
	)

	winners := 0
	conflicts := 0
	winnerIndex := -1

	for i, response := range results {
		if response.Error == nil {
			requireKVDedupResult(
				t, response, true, commands[i].Value,
			)

			winners++
			winnerIndex = i
			continue
		}

		requireKVValidationCode(
			t, response.Error,
			shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
		)

		if response.Applied || response.Value != "" {
			t.Fatalf("invalid conflict response: %v", response)
		}

		conflicts++
	}

	if winners != 1 || conflicts != 2 {
		t.Fatalf(
			"winners=%d conflicts=%d",
			winners, conflicts,
		)
	}

	winner := commands[winnerIndex]

	requireKVDedupValue(
		t, node, "alpha", winner.Value,
		winner.Op != kvstore.OpDelete,
	)

	requireKVDedupResult(
		t,
		invokeKVDedupMutation(t, server, winner),
		true, winner.Value,
	)

	if kvDedupCommit(t, node) != before+1 {
		t.Fatal("cross-operation conflict advanced commit more than once")
	}

	t.Logf("winning operation=%s", winner.Op)
}

func TestKVGRPCConcurrentIndependentRequests(t *testing.T) {
	node, server := newKVDedupShard(t)

	commands := make(
		[]kvstore.Command,
		kvConcurrentWorkers,
	)

	for i := range commands {
		commands[i] = kvDedupPut(
			fmt.Sprintf("request-%d", i),
			fmt.Sprintf("key-%d", i),
			fmt.Sprintf("value-%d", i),
		)
	}

	before := kvDedupCommit(t, node)

	results := runKVConcurrentMutations(
		t, server, commands,
	)

	for i, response := range results {
		command := commands[i]

		requireKVDedupResult(
			t, response, true, command.Value,
		)

		requireKVDedupValue(
			t, node, command.Key, command.Value, true,
		)
	}

	after := kvDedupCommit(t, node)

	if after != before+uint64(len(commands)) {
		t.Fatalf(
			"expected %d commits, got %d",
			len(commands), after-before,
		)
	}

	t.Logf(
		"workers=%d successful=%d commit_delta=%d",
		len(commands), len(results), after-before,
	)
}

func TestKVGRPCConcurrentCASAtomicity(t *testing.T) {
	node, server := newKVDedupShard(t)

	requireKVDedupResult(t,
		invokeKVDedupMutation(t, server,
			kvDedupPut("seed", "alpha", "100")),
		true, "100",
	)

	commands := make(
		[]kvstore.Command,
		kvConcurrentWorkers,
	)

	for i := range commands {
		command := kvDedupPut(
			fmt.Sprintf("cas-%d", i),
			"alpha",
			fmt.Sprintf("winner-%d", i),
		)

		command.Op = kvstore.OpCompareAndSwap
		command.Expected = "100"

		commands[i] = command
	}

	before := kvDedupCommit(t, node)

	results := runKVConcurrentMutations(
		t, server, commands,
	)

	applied := 0
	mismatched := 0
	winnerValue := ""

	for i, response := range results {
		if response.Error != nil {
			t.Fatalf("unexpected CAS error: %v", response.Error)
		}

		if response.Applied {
			requireKVDedupResult(
				t, response, true, commands[i].Value,
			)

			applied++
			winnerValue = response.Value
		} else {
			requireKVDedupResult(t, response, false, "")
			mismatched++
		}
	}

	if applied != 1 || mismatched != len(commands)-1 {
		t.Fatalf(
			"applied=%d mismatched=%d",
			applied, mismatched,
		)
	}

	requireKVDedupValue(t, node, "alpha", winnerValue, true)

	after := kvDedupCommit(t, node)

	if after != before+uint64(len(commands)) {
		t.Fatalf(
			"expected %d committed CAS results, got %d",
			len(commands), after-before,
		)
	}

	// Every retry must preserve its original comparison result.
	retries := runKVConcurrentMutations(
		t, server, commands,
	)

	for i, response := range retries {
		requireKVDedupResult(
			t, response,
			results[i].Applied,
			results[i].Value,
		)
	}

	if kvDedupCommit(t, node) != after {
		t.Fatal("CAS retries advanced commit index")
	}

	requireKVDedupValue(t, node, "alpha", winnerValue, true)

	t.Logf(
		"workers=%d applied=%d mismatched=%d commit_delta=%d",
		len(commands), applied, mismatched, after-before,
	)
}
