package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type crossComponentCLIResponse struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Found     bool   `json:"found"`
	Applied   bool   `json:"applied"`
	ReadMode  string `json:"read_mode"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

func TestGatewayDistributedCrossComponent(t *testing.T) {
	if os.Getenv("SHARDRAFT_GATEWAY_INTEGRATION") != "1" {
		t.Skip("Set SHARDRAFT_GATEWAY_INTEGRATION=1")
	}

	cliBinary := os.Getenv("SHARDRAFT_CLI_BINARY")

	if cliBinary == "" {
		t.Fatal("SHARDRAFT_CLI_BINARY is required")
	}

	cluster := newGatewayDistributedCluster(t)

	var endpoints []string

	for _, node := range cluster.nodes {
		endpoints = append(endpoints, node.grpcAddr)
	}

	const (
		key      = "cross-component-key"
		path     = "/kv/" + key
		clientID = "cross-component-client"
	)

	runCLI := func(
		expectedExit int,
		args ...string,
	) crossComponentCLIResponse {
		t.Helper()

		ctx, cancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cancel()

		commandArgs := append(
			[]string{
				"--grpc-endpoints",
				strings.Join(endpoints, ","),
				"--timeout", "11s",
				"--output", "json",
			},
			args...,
		)

		command := exec.CommandContext(
			ctx,
			cliBinary,
			commandArgs...,
		)

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		command.Stdout = &stdout
		command.Stderr = &stderr

		err := command.Run()

		if ctx.Err() != nil {
			t.Fatalf("CLI timeout: %v", ctx.Err())
		}

		exitCode := 0

		if err != nil {
			exitError, ok := err.(*exec.ExitError)

			if !ok {
				t.Fatal(err)
			}

			exitCode = exitError.ExitCode()
		}

		if exitCode != expectedExit ||
			stderr.Len() != 0 {
			t.Fatalf(
				"CLI exit=%d want=%d stdout=%s stderr=%s",
				exitCode,
				expectedExit,
				stdout.String(),
				stderr.String(),
			)
		}

		var result crossComponentCLIResponse

		if err := json.Unmarshal(
			stdout.Bytes(),
			&result,
		); err != nil {
			t.Fatal(err)
		}

		return result
	}

	cliMutation := func(
		requestID string,
		operation string,
		expectedValue string,
		args ...string,
	) crossComponentCLIResponse {
		t.Helper()

		commandArgs := []string{
			"--client-id", clientID,
			"--request-id", requestID,
			operation,
			key,
		}

		commandArgs = append(commandArgs, args...)

		result := runCLI(0, commandArgs...)

		if !result.Applied ||
			result.Operation != operation ||
			result.Key != key ||
			result.Value != expectedValue ||
			result.ClientID != clientID ||
			result.RequestID != requestID {
			t.Fatalf(
				"Unexpected CLI mutation: %+v",
				result,
			)
		}

		return result
	}

	gatewayMutation := func(
		method string,
		requestPath string,
		requestID string,
		fields map[string]string,
		expectedStatus int,
	) gatewayDistributedResponse {
		t.Helper()

		body := map[string]string{
			"client_id":  clientID,
			"request_id": requestID,
		}

		for name, value := range fields {
			body[name] = value
		}

		result := cluster.request(
			t,
			method,
			requestPath,
			body,
			expectedStatus,
		)

		if expectedStatus == http.StatusOK {
			if !result.Applied ||
				result.ClientID != clientID ||
				result.RequestID != requestID ||
				result.Error != "" {
				t.Fatalf(
					"Unexpected gateway mutation: %+v",
					result,
				)
			}
		}

		return result
	}

	requireGatewayValue := func(value string) {
		t.Helper()

		result := cluster.request(
			t,
			http.MethodGet,
			path,
			nil,
			http.StatusOK,
		)

		if !result.Found ||
			result.Key != key ||
			result.Value != value {
			t.Fatalf(
				"Unexpected gateway read: %+v",
				result,
			)
		}
	}

	requireCLIValue := func(value string) {
		t.Helper()

		result := runCLI(0, "get", key)

		if !result.Found ||
			result.Key != key ||
			result.Value != value ||
			result.ReadMode != "linearizable" {
			t.Fatalf(
				"Unexpected CLI read: %+v",
				result,
			)
		}
	}

	leader := cluster.waitLeader(t, 0)

	initial, err := cluster.status(leader)
	if err != nil {
		t.Fatal(err)
	}

	requireCommitDelta := func(delta uint64) {
		t.Helper()

		currentLeader := cluster.waitLeader(t, 0)

		status, err := cluster.status(currentLeader)
		if err != nil {
			t.Fatal(err)
		}

		if status.Term != initial.Term ||
			status.LeaderID != initial.LeaderID ||
			status.CommitIndex != initial.CommitIndex+delta ||
			status.AppliedIndex != status.CommitIndex {
			t.Fatalf(
				"Unexpected commit progression: initial=%+v current=%+v delta=%d",
				initial,
				status,
				delta,
			)
		}

		t.Logf(
			"Commit progression verified: delta=%d",
			delta,
		)
	}

	// 1. CLI writes are visible through the gateway.

	original := cliMutation(
		"put-001",
		"put",
		"100",
		"100",
	)

	requireGatewayValue("100")
	requireCommitDelta(1)

	t.Log("PASS: CLI PUT -> Gateway GET")

	// 2. Gateway writes are visible through the CLI.

	update := gatewayMutation(
		http.MethodPut,
		path,
		"put-002",
		map[string]string{"value": "200"},
		http.StatusOK,
	)

	if update.Value != "200" {
		t.Fatalf("Unexpected gateway value: %+v", update)
	}

	requireCLIValue("200")
	requireCommitDelta(2)

	t.Log("PASS: Gateway PUT -> CLI GET")

	// 3. Replay the original CLI mutation through the gateway.

	replayed := gatewayMutation(
		http.MethodPut,
		path,
		"put-001",
		map[string]string{"value": "100"},
		http.StatusOK,
	)

	if replayed.Value != original.Value ||
		replayed.Applied != original.Applied ||
		replayed.ClientID != original.ClientID ||
		replayed.RequestID != original.RequestID {
		t.Fatalf("Unexpected cross-interface replay: %+v", replayed)
	}

	requireCLIValue("200")
	requireCommitDelta(2)

	t.Log("PASS: CLI mutation deduplicated through gateway")

	// 4. Replay a gateway mutation through the CLI.

	cliReplay := cliMutation(
		"put-002",
		"put",
		"200",
		"200",
	)

	if cliReplay.Value != update.Value ||
		cliReplay.Applied != update.Applied {
		t.Fatalf("Unexpected CLI replay: %+v", cliReplay)
	}

	requireGatewayValue("200")
	requireCommitDelta(2)

	t.Log("PASS: Gateway mutation deduplicated through CLI")

	// 5. Reject conflicting reuse of a CLI mutation identity.

	conflict := gatewayMutation(
		http.MethodPut,
		path,
		"put-001",
		map[string]string{"value": "999"},
		http.StatusConflict,
	)

	if !strings.Contains(
		strings.ToLower(conflict.Code+" "+conflict.Error),
		"conflict",
	) {
		t.Fatalf("Unexpected conflict response: %+v", conflict)
	}

	requireCLIValue("200")
	requireCommitDelta(2)

	t.Log("PASS: Cross-interface identity conflict rejected")

	// 6. CLI compare-and-swap is visible through the gateway.

	cliMutation(
		"cas-001",
		"cas",
		"300",
		"200",
		"300",
	)

	requireGatewayValue("300")
	requireCommitDelta(3)

	t.Log("PASS: CLI CAS -> Gateway GET")

	// 7. Verify compatibility with the legacy daemon HTTP API.

	leader = cluster.waitLeader(t, 0)

	legacyResponse, err := cluster.httpClient.Get(
		"http://" + leader.httpAddr + path,
	)

	if err != nil {
		t.Fatal(err)
	}

	defer legacyResponse.Body.Close()

	if legacyResponse.StatusCode != http.StatusOK {
		t.Fatalf(
			"Legacy HTTP GET status=%d",
			legacyResponse.StatusCode,
		)
	}

	var legacy struct {
		Value string `json:"value"`
	}

	if err := json.NewDecoder(
		legacyResponse.Body,
	).Decode(&legacy); err != nil {
		t.Fatal(err)
	}

	if legacy.Value != "300" {
		t.Fatalf("Unexpected legacy HTTP value: %+v", legacy)
	}

	requireCommitDelta(3)

	t.Log("PASS: Legacy daemon HTTP API compatibility")

	// 8. Gateway DELETE is visible through the CLI.

	gatewayMutation(
		http.MethodDelete,
		path,
		"delete-001",
		nil,
		http.StatusOK,
	)

	missing := runCLI(3, "get", key)

	if missing.Found ||
		missing.Key != key ||
		missing.Value != "" ||
		missing.ReadMode != "linearizable" {
		t.Fatalf("Unexpected missing response: %+v", missing)
	}

	gatewayMissing := cluster.request(
		t,
		http.MethodGet,
		path,
		nil,
		http.StatusNotFound,
	)

	if gatewayMissing.Code != "key_not_found" {
		t.Fatalf(
			"Unexpected gateway missing response: %+v",
			gatewayMissing,
		)
	}

	requireCommitDelta(4)

	t.Log("PASS: Gateway DELETE -> CLI GET not found")
	t.Log("PASS: Exactly four additional committed entries")
	t.Log("PASS: Cross-component integration completed")
}
