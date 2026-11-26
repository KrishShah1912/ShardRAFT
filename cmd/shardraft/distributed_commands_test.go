package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func cliDistributedRunCommand(
	t *testing.T,
	cluster *cliDistributedCluster,
	endpoints string,
	expectedExit int,
	args ...string,
) cliDistributedResult {
	t.Helper()

	commandArgs := []string{
		"--grpc-endpoints", endpoints,
		"--timeout", "11s",
		"--output", "json",
	}

	commandArgs = append(commandArgs, args...)

	result := cluster.execute(t, commandArgs...)

	cliDistributedRequireExit(
		t,
		result,
		expectedExit,
	)

	if result.Stderr != "" {
		t.Fatalf(
			"Unexpected stderr: args=%q stderr=%q",
			args,
			result.Stderr,
		)
	}

	return result
}

func cliDistributedRead(
	t *testing.T,
	cluster *cliDistributedCluster,
	endpoints string,
	key string,
	expectedValue string,
	expectedFound bool,
	local bool,
) cliGetResult {
	t.Helper()

	args := []string{"get"}
	mode := "linearizable"

	if local {
		args = append(args, "--local")
		mode = "local"
	}

	args = append(args, "--", key)

	expectedExit := cliExitSuccess

	if !expectedFound {
		expectedExit = cliExitNotFound
	}

	response := cliDistributedRunCommand(
		t,
		cluster,
		endpoints,
		expectedExit,
		args...,
	)

	var result cliGetResult

	if err := json.Unmarshal(
		[]byte(response.Stdout),
		&result,
	); err != nil {
		t.Fatalf(
			"Invalid GET JSON: %q: %v",
			response.Stdout,
			err,
		)
	}

	if result.Key != key ||
		result.Value != expectedValue ||
		result.Found != expectedFound ||
		result.ReadMode != mode {
		t.Fatalf(
			"Unexpected GET result: %+v want key=%q value=%q found=%v mode=%s",
			result,
			key,
			expectedValue,
			expectedFound,
			mode,
		)
	}

	return result
}

func cliDistributedMutate(
	t *testing.T,
	cluster *cliDistributedCluster,
	endpoints string,
	clientID string,
	requestID string,
	operation cliOperation,
	key string,
	expectedApplied bool,
	values ...string,
) cliMutationResult {
	t.Helper()

	var args []string

	if clientID != "" {
		args = append(
			args,
			"--client-id", clientID,
		)
	}

	if requestID != "" {
		args = append(
			args,
			"--request-id", requestID,
		)
	}

	args = append(
		args,
		string(operation),
		"--",
		key,
	)

	args = append(args, values...)

	response := cliDistributedRunCommand(
		t,
		cluster,
		endpoints,
		cliExitSuccess,
		args...,
	)

	var result cliMutationResult

	if err := json.Unmarshal(
		[]byte(response.Stdout),
		&result,
	); err != nil {
		t.Fatalf(
			"Invalid mutation JSON: %q: %v",
			response.Stdout,
			err,
		)
	}

	if result.Operation != operation ||
		result.Key != key ||
		result.Applied != expectedApplied ||
		result.ClientID == "" ||
		result.RequestID == "" {
		t.Fatalf(
			"Unexpected mutation result: %+v",
			result,
		)
	}

	if clientID != "" &&
		result.ClientID != clientID {
		t.Fatal("Client identity changed")
	}

	if requestID != "" &&
		result.RequestID != requestID {
		t.Fatal("Request identity changed")
	}

	if expectedApplied {
		switch operation {
		case cliPut, cliCAS:
			expectedValue := values[len(values)-1]

			if result.Value != expectedValue {
				t.Fatalf(
					"Mutation value=%q want=%q",
					result.Value,
					expectedValue,
				)
			}

		case cliDelete:
			if result.Value != "" {
				t.Fatalf(
					"DELETE returned unexpected value: %q",
					result.Value,
				)
			}
		}
	}

	return result
}

func cliDistributedRequireCommitAdvance(
	t *testing.T,
	cluster *cliDistributedCluster,
	before cliDistributedStatus,
	delta uint64,
) cliDistributedStatus {
	t.Helper()

	expected := before.CommitIndex + delta

	after := cluster.waitConvergence(
		t,
		expected,
	)

	if after.Term != before.Term ||
		after.LeaderID != before.LeaderID ||
		after.CommitIndex != expected ||
		after.AppliedIndex != expected {
		t.Fatalf(
			"Unexpected Raft advancement: before=%+v after=%+v expected_delta=%d",
			before,
			after,
			delta,
		)
	}

	t.Logf(
		"Raft commit=%d -> %d delta=%d",
		before.CommitIndex,
		after.CommitIndex,
		delta,
	)

	return after
}

func TestCLIDistributedCRUD(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	leader := cluster.waitLeader(t, 0)
	follower := cluster.follower(t, leader)

	endpoints := follower.grpcAddr

	const key = "account"
	const clientID = "distributed-crud-client"

	before := cluster.waitConvergence(t, 1)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "", false, false,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "put-100",
		cliPut, key, true, "100",
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "100", true, false,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "put-200",
		cliPut, key, true, "200",
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "200", true, false,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "delete-account",
		cliDelete, key, true,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "", false, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, before, 3,
	)

	t.Log("Distributed CRUD passed through follower bootstrap")
}

func TestCLIDistributedCAS(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const key = "cas-account"
	const clientID = "distributed-cas-client"

	before := cluster.waitConvergence(t, 1)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "seed",
		cliPut, key, true, "100",
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas-success",
		cliCAS, key, true,
		"100", "200",
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "200", true, false,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas-mismatch",
		cliCAS, key, false,
		"100", "300",
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "200", true, false,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas-second-success",
		cliCAS, key, true,
		"200", "300",
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "300", true, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, before, 4,
	)

	t.Log("CAS success and mismatch behavior passed")
	t.Log("CAS mismatch preserved the stored value")
}

func TestCLIDistributedReadModes(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	const key = "read-mode-key"
	const value = "replicated-value"

	before := cluster.waitConvergence(t, 1)

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		"", "",
		cliPut, key, true, value,
	)

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 1,
	)

	for _, node := range cluster.nodes {
		t.Run(
			fmt.Sprintf("Node%d", node.id),
			func(t *testing.T) {
				cliDistributedRead(
					t, cluster, node.grpcAddr,
					key, value, true, false,
				)

				cliDistributedRead(
					t, cluster, node.grpcAddr,
					key, value, true, true,
				)

				cliDistributedRead(
					t, cluster, node.grpcAddr,
					"missing-read-mode-key",
					"", false, false,
				)

				cliDistributedRead(
					t, cluster, node.grpcAddr,
					"missing-read-mode-key",
					"", false, true,
				)
			},
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	t.Log("Default linearizable and explicit local reads passed")
	t.Log("12 read operations completed across three bootstrap endpoints")
	t.Log("Reads committed no additional Raft entries")
}

func TestCLIDistributedSpecialValues(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	cases := []struct {
		name  string
		key   string
		value string
	}{
		{
			name:  "EmptyValue",
			key:   "empty-value",
			value: "",
		},
		{
			name:  "Whitespace",
			key:   "key with spaces",
			value: "  value with spaces  ",
		},
		{
			name:  "Unicode",
			key:   "unicode-\u4e16\u754c",
			value: "hello-\u4e16\u754c-\U0001F680",
		},
		{
			name:  "Newlines",
			key:   "multiline-key",
			value: "first\nsecond\tthird",
		},
		{
			name:  "LeadingDash",
			key:   "--special-key",
			value: "--special-value",
		},
		{
			name:  "JSONCharacters",
			key:   "json-key",
			value: `{"name":"test","path":"a\\b"}`,
		},
	}

	before := cluster.waitConvergence(t, 1)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cliDistributedMutate(
				t, cluster, endpoints,
				"", "",
				cliPut, tc.key, true, tc.value,
			)

			cliDistributedRead(
				t, cluster, endpoints,
				tc.key, tc.value, true, false,
			)

			updated := tc.value + "-updated"

			cliDistributedMutate(
				t, cluster, endpoints,
				"", "",
				cliCAS, tc.key, true,
				tc.value, updated,
			)

			cliDistributedRead(
				t, cluster, endpoints,
				tc.key, updated, true, false,
			)

			cliDistributedMutate(
				t, cluster, endpoints,
				"", "",
				cliDelete, tc.key, true,
			)

			cliDistributedRead(
				t, cluster, endpoints,
				tc.key, "", false, false,
			)
		})
	}

	cliDistributedRequireCommitAdvance(
		t,
		cluster,
		before,
		uint64(len(cases)*3),
	)

	t.Logf(
		"Validated %d special-value cases",
		len(cases),
	)
}

func TestCLIDistributedAutomaticIdentities(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	before := cluster.waitConvergence(t, 1)

	clients := make(map[string]bool)
	requests := make(map[string]bool)

	const operations = 20

	for i := 0; i < operations; i++ {
		key := fmt.Sprintf("identity-key-%d", i)
		value := fmt.Sprintf("value-%d", i)

		result := cliDistributedMutate(
			t, cluster, endpoints,
			"", "",
			cliPut, key, true, value,
		)

		if clients[result.ClientID] {
			t.Fatal("Duplicate generated client ID")
		}

		if requests[result.RequestID] {
			t.Fatal("Duplicate generated request ID")
		}

		clients[result.ClientID] = true
		requests[result.RequestID] = true

		cliDistributedRead(
			t, cluster, endpoints,
			key, value, true, false,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, before, operations,
	)

	t.Logf(
		"Independent CLI invocations=%d unique_clients=%d unique_requests=%d",
		operations,
		len(clients),
		len(requests),
	)
}

func TestCLIDistributedTextOutput(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const key = "text-key"

	before := cluster.waitConvergence(t, 1)

	run := func(
		expectedExit int,
		args ...string,
	) string {
		t.Helper()

		commandArgs := []string{
			"--grpc-endpoints", endpoints,
			"--timeout", "11s",
			"--output", "text",
		}

		commandArgs = append(
			commandArgs,
			args...,
		)

		result := cluster.execute(
			t,
			commandArgs...,
		)

		cliDistributedRequireExit(
			t, result, expectedExit,
		)

		if result.Stderr != "" {
			t.Fatalf(
				"Unexpected stderr: %q",
				result.Stderr,
			)
		}

		return result.Stdout
	}

	requireFields := func(
		output string,
		fields ...string,
	) {
		t.Helper()

		for _, field := range fields {
			if !strings.Contains(output, field) {
				t.Fatalf(
					"Missing field %q in output %q",
					field,
					output,
				)
			}
		}
	}

	put := run(
		cliExitSuccess,
		"--client-id", "text-client",
		"--request-id", "text-put",
		"put", key, "100",
	)

	requireFields(
		put,
		"Operation: put",
		`Key: "text-key"`,
		`Value: "100"`,
		"Applied: true",
		`Client ID: "text-client"`,
		`Request ID: "text-put"`,
	)

	get := run(
		cliExitSuccess,
		"get", key,
	)

	requireFields(
		get,
		`Key: "text-key"`,
		`Value: "100"`,
		"Found: true",
		"Read mode: linearizable",
	)

	cas := run(
		cliExitSuccess,
		"--client-id", "text-client",
		"--request-id", "text-cas",
		"cas", key, "100", "200",
	)

	requireFields(
		cas,
		"Operation: cas",
		"Applied: true",
		`Value: "200"`,
	)

	mismatch := run(
		cliExitSuccess,
		"--client-id", "text-client",
		"--request-id", "text-mismatch",
		"cas", key, "100", "300",
	)

	requireFields(
		mismatch,
		"Operation: cas",
		"Applied: false",
	)

	deleted := run(
		cliExitSuccess,
		"--client-id", "text-client",
		"--request-id", "text-delete",
		"delete", key,
	)

	requireFields(
		deleted,
		"Operation: delete",
		"Applied: true",
	)

	missing := run(
		cliExitNotFound,
		"get", key,
	)

	requireFields(
		missing,
		`Key: "text-key"`,
		"Found: false",
		"Read mode: linearizable",
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, before, 4,
	)

	t.Log("Real CLI text output passed for all four commands")
}
