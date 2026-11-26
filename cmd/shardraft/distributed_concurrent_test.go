package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

const cliDistributedConcurrentWorkers = 16

type cliConcurrentCommand struct {
	clientID  string
	requestID string
	operation cliOperation
	key       string
	values    []string
}

func cliConcurrentRun(
	t *testing.T,
	cluster *cliDistributedCluster,
	commands [][]string,
) []cliDistributedResult {
	t.Helper()

	results := make([]cliDistributedResult, len(commands))
	failures := make([]error, len(commands))

	start := make(chan struct{})

	var ready sync.WaitGroup
	var finished sync.WaitGroup

	ready.Add(len(commands))
	finished.Add(len(commands))

	for i, args := range commands {
		go func(index int, args []string) {
			defer finished.Done()

			ready.Done()
			<-start

			ctx, cancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			defer cancel()

			commandArgs := []string{
				"--grpc-endpoints",
				cluster.nodes[index%len(cluster.nodes)].grpcAddr,
				"--timeout", "11s",
				"--output", "json",
			}

			commandArgs = append(commandArgs, args...)

			var stdout bytes.Buffer
			var stderr bytes.Buffer

			command := exec.CommandContext(
				ctx,
				cluster.cliBinary,
				commandArgs...,
			)

			command.Stdout = &stdout
			command.Stderr = &stderr

			err := command.Run()

			result := cliDistributedResult{
				Stdout: stdout.String(),
				Stderr: stderr.String(),
			}

			if ctx.Err() != nil {
				failures[index] = ctx.Err()
				return
			}

			if err != nil {
				var exitError *exec.ExitError

				if !errors.As(err, &exitError) {
					failures[index] = err
					return
				}

				result.ExitCode = exitError.ExitCode()

				if result.ExitCode < 0 {
					failures[index] = err
					return
				}
			}

			results[index] = result
		}(i, args)
	}

	ready.Wait()
	close(start)
	finished.Wait()

	for i, err := range failures {
		if err != nil {
			t.Fatalf(
				"Worker %d execution failed: %v",
				i, err,
			)
		}
	}

	return results
}

func cliConcurrentMutations(
	t *testing.T,
	cluster *cliDistributedCluster,
	commands []cliConcurrentCommand,
) []cliDistributedResult {
	t.Helper()

	args := make([][]string, len(commands))

	for i, command := range commands {
		args[i] = []string{
			"--client-id", command.clientID,
			"--request-id", command.requestID,
			string(command.operation),
			"--",
			command.key,
		}

		args[i] = append(args[i], command.values...)
	}

	return cliConcurrentRun(t, cluster, args)
}

func cliConcurrentRequireMutation(
	t *testing.T,
	response cliDistributedResult,
	command cliConcurrentCommand,
) cliMutationResult {
	t.Helper()

	cliDistributedRequireExit(
		t, response, cliExitSuccess,
	)

	if response.Stderr != "" {
		t.Fatalf("Unexpected stderr: %q", response.Stderr)
	}

	var result cliMutationResult

	if err := json.Unmarshal(
		[]byte(response.Stdout),
		&result,
	); err != nil {
		t.Fatalf(
			"Invalid mutation response: %q: %v",
			response.Stdout, err,
		)
	}

	if result.Operation != command.operation ||
		result.Key != command.key ||
		result.ClientID != command.clientID ||
		result.RequestID != command.requestID {
		t.Fatalf(
			"Unexpected mutation response: %+v",
			result,
		)
	}

	if result.Applied {
		expectedValue := ""

		if command.operation != cliDelete {
			expectedValue = command.values[len(command.values)-1]
		}

		if result.Value != expectedValue {
			t.Fatalf(
				"Mutation value=%q want=%q",
				result.Value, expectedValue,
			)
		}
	}

	return result
}

func cliConcurrentRepeat(
	command cliConcurrentCommand,
) []cliConcurrentCommand {
	commands := make(
		[]cliConcurrentCommand,
		cliDistributedConcurrentWorkers,
	)

	for i := range commands {
		commands[i] = command
	}

	return commands
}

func cliConcurrentIdentical(
	t *testing.T,
	operation cliOperation,
	seed bool,
	expectedApplied bool,
	expectedFound bool,
	expectedValue string,
	values ...string,
) {
	t.Helper()

	cluster := newCLIDistributedCluster(t)

	const key = "concurrent-identical-key"
	const clientID = "concurrent-identical-client"

	if seed {
		cliDistributedMutate(
			t, cluster, cluster.endpoints(),
			clientID, "seed",
			cliPut, key, true, "100",
		)
	}

	before := cluster.waitConvergence(t, 1)

	command := cliConcurrentCommand{
		clientID:  clientID,
		requestID: "shared-request",
		operation: operation,
		key:       key,
		values:    values,
	}

	responses := cliConcurrentMutations(
		t, cluster,
		cliConcurrentRepeat(command),
	)

	var original cliMutationResult

	for i, response := range responses {
		result := cliConcurrentRequireMutation(
			t, response, command,
		)

		if result.Applied != expectedApplied {
			t.Fatalf(
				"Worker %d applied=%v want=%v",
				i, result.Applied, expectedApplied,
			)
		}

		if i == 0 {
			original = result
		} else if result != original {
			t.Fatalf(
				"Worker %d response differs: %+v vs %+v",
				i, result, original,
			)
		}
	}

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 1,
	)

	for _, node := range cluster.nodes {
		cliDistributedRead(
			t, cluster, node.grpcAddr,
			key, expectedValue,
			expectedFound, false,
		)

		replayed := cliDistributedMutate(
			t, cluster, node.grpcAddr,
			command.clientID,
			command.requestID,
			command.operation,
			command.key,
			expectedApplied,
			command.values...,
		)

		cliDistributedRequireReplay(
			t, original, replayed,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	t.Logf(
		"operation=%s workers=%d identical_responses=%d commit_delta=1",
		operation, len(responses), len(responses),
	)
}

func TestCLIDistributedConcurrentIdenticalPut(t *testing.T) {
	cliConcurrentIdentical(
		t, cliPut,
		false, true, true,
		"200", "200",
	)
}

func TestCLIDistributedConcurrentIdenticalDelete(t *testing.T) {
	cliConcurrentIdentical(
		t, cliDelete,
		true, true, false,
		"",
	)
}

func TestCLIDistributedConcurrentIdenticalCASMatch(t *testing.T) {
	cliConcurrentIdentical(
		t, cliCAS,
		true, true, true,
		"200", "100", "200",
	)
}

func TestCLIDistributedConcurrentIdenticalCASMismatch(t *testing.T) {
	cliConcurrentIdentical(
		t, cliCAS,
		true, false, true,
		"100", "999", "200",
	)
}

func TestCLIDistributedConcurrentIndependentPut(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	before := cluster.waitConvergence(t, 1)

	commands := make(
		[]cliConcurrentCommand,
		cliDistributedConcurrentWorkers,
	)

	for i := range commands {
		commands[i] = cliConcurrentCommand{
			clientID:  fmt.Sprintf("client-%d", i),
			requestID: fmt.Sprintf("request-%d", i),
			operation: cliPut,
			key:       fmt.Sprintf("independent-key-%d", i),
			values:    []string{fmt.Sprintf("value-%d", i)},
		}
	}

	responses := cliConcurrentMutations(
		t, cluster, commands,
	)

	for i, response := range responses {
		result := cliConcurrentRequireMutation(
			t, response, commands[i],
		)

		if !result.Applied {
			t.Fatalf("Independent PUT %d was not applied", i)
		}
	}

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, uint64(len(commands)),
	)

	for i, command := range commands {
		cliDistributedRead(
			t, cluster,
			cluster.nodes[i%len(cluster.nodes)].grpcAddr,
			command.key,
			command.values[0],
			true, false,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	t.Logf(
		"workers=%d successful=%d commit_delta=%d",
		len(commands), len(commands), len(commands),
	)
}

func TestCLIDistributedConcurrentConflictingPut(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	before := cluster.waitConvergence(t, 1)

	commands := make(
		[]cliConcurrentCommand,
		cliDistributedConcurrentWorkers,
	)

	for i := range commands {
		commands[i] = cliConcurrentCommand{
			clientID:  "conflicting-client",
			requestID: "shared-request",
			operation: cliPut,
			key:       "conflicting-key",
			values:    []string{fmt.Sprintf("value-%d", i)},
		}
	}

	responses := cliConcurrentMutations(
		t, cluster, commands,
	)

	winners := 0
	conflicts := 0
	winnerIndex := -1

	var winner cliMutationResult

	for i, response := range responses {
		if response.ExitCode == cliExitSuccess {
			result := cliConcurrentRequireMutation(
				t, response, commands[i],
			)

			if !result.Applied {
				t.Fatal("Winning PUT was not applied")
			}

			winners++
			winnerIndex = i
			winner = result
			continue
		}

		cliDistributedRequireExit(
			t, response, cliExitFailure,
		)

		if response.Stdout != "" ||
			!strings.Contains(
				strings.ToLower(response.Stderr),
				"conflict",
			) ||
			!strings.Contains(
				response.Stderr,
				commands[i].clientID,
			) ||
			!strings.Contains(
				response.Stderr,
				commands[i].requestID,
			) {
			t.Fatalf(
				"Invalid conflict response: %+v",
				response,
			)
		}

		conflicts++
	}

	if winners != 1 || conflicts != len(commands)-1 {
		t.Fatalf(
			"winners=%d conflicts=%d",
			winners, conflicts,
		)
	}

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 1,
	)

	command := commands[winnerIndex]

	cliDistributedRead(
		t, cluster, cluster.endpoints(),
		command.key, winner.Value,
		true, false,
	)

	replayed := cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		command.clientID,
		command.requestID,
		command.operation,
		command.key,
		true,
		command.values...,
	)

	cliDistributedRequireReplay(
		t, winner, replayed,
	)

	for i, rejected := range commands {
		if i == winnerIndex {
			continue
		}

		cliDistributedRequireConflict(
			t, cluster, cluster.endpoints(),
			rejected.clientID,
			rejected.requestID,
			"put", rejected.key,
			rejected.values[0],
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	t.Logf(
		"workers=%d winners=%d conflicts=%d commit_delta=1",
		len(commands), winners, conflicts,
	)
}

func TestCLIDistributedConcurrentCompetingCAS(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	const key = "competing-cas-key"

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		"cas-client", "seed",
		cliPut, key, true, "100",
	)

	before := cluster.waitConvergence(t, 2)

	commands := make(
		[]cliConcurrentCommand,
		cliDistributedConcurrentWorkers,
	)

	for i := range commands {
		commands[i] = cliConcurrentCommand{
			clientID:  "cas-client",
			requestID: fmt.Sprintf("cas-%d", i),
			operation: cliCAS,
			key:       key,
			values: []string{
				"100",
				fmt.Sprintf("winner-%d", i),
			},
		}
	}

	responses := cliConcurrentMutations(
		t, cluster, commands,
	)

	results := make([]cliMutationResult, len(commands))

	winners := 0
	mismatches := 0
	winnerValue := ""

	for i, response := range responses {
		result := cliConcurrentRequireMutation(
			t, response, commands[i],
		)

		results[i] = result

		if result.Applied {
			winners++
			winnerValue = result.Value
		} else {
			mismatches++
		}
	}

	if winners != 1 || mismatches != len(commands)-1 {
		t.Fatalf(
			"CAS winners=%d mismatches=%d",
			winners, mismatches,
		)
	}

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, uint64(len(commands)),
	)

	for _, node := range cluster.nodes {
		cliDistributedRead(
			t, cluster, node.grpcAddr,
			key, winnerValue, true, false,
		)
	}

	for i, command := range commands {
		replayed := cliDistributedMutate(
			t, cluster, cluster.endpoints(),
			command.clientID,
			command.requestID,
			command.operation,
			command.key,
			results[i].Applied,
			command.values...,
		)

		cliDistributedRequireReplay(
			t, results[i], replayed,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	t.Logf(
		"workers=%d winners=%d mismatches=%d commit_delta=%d",
		len(commands), winners, mismatches, len(commands),
	)
}

func TestCLIDistributedConcurrentReads(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	const key = "concurrent-read-key"
	const value = "committed-value"

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		"read-client", "seed",
		cliPut, key, true, value,
	)

	before := cluster.waitConvergence(t, 2)

	commands := make(
		[][]string,
		cliDistributedConcurrentWorkers,
	)

	for i := range commands {
		if i%2 == 0 {
			commands[i] = []string{"get", "--", key}
		} else {
			commands[i] = []string{
				"get", "--", "missing-concurrent-key",
			}
		}
	}

	responses := cliConcurrentRun(
		t, cluster, commands,
	)

	for i, response := range responses {
		expectedFound := i%2 == 0
		expectedExit := cliExitSuccess
		expectedKey := key
		expectedValue := value

		if !expectedFound {
			expectedExit = cliExitNotFound
			expectedKey = "missing-concurrent-key"
			expectedValue = ""
		}

		cliDistributedRequireExit(
			t, response, expectedExit,
		)

		if response.Stderr != "" {
			t.Fatalf(
				"Unexpected read stderr: %q",
				response.Stderr,
			)
		}

		var result cliGetResult

		if err := json.Unmarshal(
			[]byte(response.Stdout),
			&result,
		); err != nil {
			t.Fatal(err)
		}

		if result.Key != expectedKey ||
			result.Value != expectedValue ||
			result.Found != expectedFound ||
			result.ReadMode != "linearizable" {
			t.Fatalf(
				"Unexpected concurrent read: %+v",
				result,
			)
		}
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, before, 0,
	)

	t.Logf(
		"concurrent_reads=%d found=%d missing=%d commit_delta=0",
		len(commands), len(commands)/2, len(commands)/2,
	)
}
