package main

import (
	"strings"
	"testing"
	"time"
)

func cliDistributedIsolateLeader(
	t *testing.T,
	cluster *cliDistributedCluster,
) (*cliDistributedNode, cliDistributedStatus) {
	t.Helper()

	before := cluster.waitConvergence(t, 1)
	leader := cluster.waitLeader(t, 0)

	for _, node := range cluster.nodes {
		if node != leader {
			node.stop(t)
		}
	}

	cluster.httpClient.CloseIdleConnections()

	status, err := cluster.status(leader)
	if err != nil {
		t.Fatal(err)
	}

	if status.CommitIndex != before.CommitIndex ||
		status.AppliedIndex != before.AppliedIndex {
		t.Fatal("Indexes changed during isolation")
	}

	t.Logf(
		"Quorum lost: surviving node=%d commit=%d applied=%d",
		leader.id,
		status.CommitIndex,
		status.AppliedIndex,
	)

	return leader, before
}

func cliDistributedRequireIsolatedIndexes(
	t *testing.T,
	cluster *cliDistributedCluster,
	leader *cliDistributedNode,
	before cliDistributedStatus,
) {
	t.Helper()

	status, err := cluster.status(leader)
	if err != nil {
		t.Fatal(err)
	}

	if status.CommitIndex != before.CommitIndex ||
		status.AppliedIndex != before.AppliedIndex {
		t.Fatalf(
			"Indexes advanced without quorum: before=%+v after=%+v",
			before, status,
		)
	}

	t.Logf(
		"Isolated indexes unchanged: commit=%d applied=%d",
		status.CommitIndex,
		status.AppliedIndex,
	)
}

func cliDistributedRestoreQuorum(
	t *testing.T,
	cluster *cliDistributedCluster,
	minimumCommit uint64,
) cliDistributedStatus {
	t.Helper()

	for _, node := range cluster.nodes {
		if node.process == nil {
			node.start(t, cluster.daemonBinary)
			cluster.waitHTTP(t, node)
		}
	}

	return cluster.waitConvergence(t, minimumCommit)
}

func TestCLIDistributedQuorumReadSafety(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	const key = "quorum-read-key"
	const value = "committed-value"

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		"quorum-read-client", "seed",
		cliPut, key, true, value,
	)

	leader, before := cliDistributedIsolateLeader(
		t, cluster,
	)

	for _, readKey := range []string{
		key,
		"quorum-missing-key",
	} {
		start := time.Now()

		result := cluster.execute(
			t,
			"--grpc-endpoints", leader.grpcAddr,
			"--timeout", "1500ms",
			"--output", "json",
			"get", readKey,
		)

		cliDistributedRequireExit(
			t, result, cliExitFailure,
		)

		if result.Stdout != "" ||
			!strings.Contains(result.Stderr, "get failed") {
			t.Fatalf(
				"Unexpected isolated read response: %+v",
				result,
			)
		}

		if time.Since(start) > 5*time.Second {
			t.Fatal("Read exceeded deadline tolerance")
		}

		t.Logf(
			"Linearizable GET rejected without quorum: key=%s",
			readKey,
		)
	}

	cliDistributedRead(
		t, cluster, leader.grpcAddr,
		key, value, true, true,
	)

	cliDistributedRead(
		t, cluster, leader.grpcAddr,
		"quorum-missing-key", "", false, true,
	)

	cliDistributedRequireIsolatedIndexes(
		t, cluster, leader, before,
	)

	after := cliDistributedRestoreQuorum(
		t, cluster, before.CommitIndex,
	)

	cliDistributedRequireRecoveryProgress(
		t,
		before.Term, before.LeaderID, before.CommitIndex,
		after.Term, after.LeaderID, after.CommitIndex,
		after.AppliedIndex, 0,
	)

	cliDistributedRead(
		t, cluster, cluster.endpoints(),
		key, value, true, false,
	)

	cliDistributedRead(
		t, cluster, cluster.endpoints(),
		"quorum-missing-key", "", false, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, after, 0,
	)

	t.Log("Verified linearizable read safety")
	t.Log("Verified local reads during quorum loss")
	t.Log("Verified read availability after recovery")
}

func cliDistributedTestQuorumMutation(
	t *testing.T,
	operation cliOperation,
	values []string,
	finalValue string,
	finalFound bool,
) {
	t.Helper()

	cluster := newCLIDistributedCluster(t)

	const key = "quorum-mutation-key"
	const preservedKey = "quorum-preserved-key"
	const clientID = "quorum-mutation-client"
	const requestID = "uncertain-request"

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		clientID, "seed",
		cliPut, key, true, "100",
	)

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		clientID, "preserved",
		cliPut, preservedKey, true, "stable",
	)

	leader, before := cliDistributedIsolateLeader(
		t, cluster,
	)

	args := []string{
		"--grpc-endpoints", leader.grpcAddr,
		"--timeout", "1500ms",
		"--output", "json",
		"--client-id", clientID,
		"--request-id", requestID,
		string(operation), "--", key,
	}

	args = append(args, values...)

	result := cluster.execute(t, args...)

	cliDistributedRequireExit(
		t, result, cliExitOutcomeUnknown,
	)

	if result.Stdout != "" {
		t.Fatalf(
			"Unexpected successful mutation output: %q",
			result.Stdout,
		)
	}

	for _, fragment := range []string{
		"Mutation outcome is unknown",
		`Client ID: "quorum-mutation-client"`,
		`Request ID: "uncertain-request"`,
		"Retry the same operation",
	} {
		if !strings.Contains(result.Stderr, fragment) {
			t.Fatalf(
				"Missing recovery information %q: %s",
				fragment, result.Stderr,
			)
		}
	}

	t.Logf(
		"Unknown outcome correctly reported: operation=%s exit=%d",
		operation, result.ExitCode,
	)

	cliDistributedRequireIsolatedIndexes(
		t, cluster, leader, before,
	)

	cliDistributedRead(
		t, cluster, leader.grpcAddr,
		key, "100", true, true,
	)

	cliDistributedRead(
		t, cluster, leader.grpcAddr,
		preservedKey, "stable", true, true,
	)

	recovered := cliDistributedRestoreQuorum(
		t, cluster, before.CommitIndex,
	)

	// The pending mutation may have committed during recovery,
	// or it may need to be proposed again using the same identity.
	replayed := cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		clientID, requestID,
		operation, key, true, values...,
	)

	after := cluster.waitConvergence(
		t, before.CommitIndex+1,
	)

	cliDistributedRequireRecoveryProgress(
		t,
		before.Term, before.LeaderID, before.CommitIndex,
		after.Term, after.LeaderID, after.CommitIndex,
		after.AppliedIndex, 1,
	)

	t.Logf(
		"Recovery and replay: recovered_commit=%d final_commit=%d",
		recovered.CommitIndex,
		after.CommitIndex,
	)

	duplicate := cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		clientID, requestID,
		operation, key, true, values...,
	)

	cliDistributedRequireReplay(
		t, replayed, duplicate,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, after, 0,
	)

	cliDistributedRead(
		t, cluster, cluster.endpoints(),
		key, finalValue, finalFound, false,
	)

	for _, node := range cluster.nodes {
		cliDistributedRead(
			t, cluster, node.grpcAddr,
			key, finalValue, finalFound, true,
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			preservedKey, "stable", true, true,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, after, 0,
	)

	t.Logf(
		"Verified quorum recovery and durable replay: operation=%s",
		operation,
	)
}

func TestCLIDistributedQuorumPutRecovery(t *testing.T) {
	cliDistributedTestQuorumMutation(
		t, cliPut,
		[]string{"200"},
		"200", true,
	)
}

func TestCLIDistributedQuorumDeleteRecovery(t *testing.T) {
	cliDistributedTestQuorumMutation(
		t, cliDelete,
		nil,
		"", false,
	)
}

func TestCLIDistributedQuorumCASRecovery(t *testing.T) {
	cliDistributedTestQuorumMutation(
		t, cliCAS,
		[]string{"100", "200"},
		"200", true,
	)
}

func TestCLIDistributedQuorumCompleteOutage(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	const key = "complete-outage-key"

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		"complete-outage-client", "seed",
		cliPut, key, true, "preserved",
	)

	before := cluster.waitConvergence(t, 2)

	for _, node := range cluster.nodes {
		node.stop(t)
	}

	cluster.httpClient.CloseIdleConnections()

	result := cluster.execute(
		t,
		"--grpc-endpoints", cluster.endpoints(),
		"--timeout", "1500ms",
		"--output", "json",
		"get", key,
	)

	cliDistributedRequireExit(
		t, result, cliExitFailure,
	)

	if result.Stdout != "" || result.Stderr == "" {
		t.Fatalf("Unexpected outage response: %+v", result)
	}

	t.Log("GET correctly failed during complete outage")

	after := cliDistributedRestoreQuorum(
		t, cluster, before.CommitIndex,
	)

	cliDistributedRequireRecoveryProgress(
		t,
		before.Term, before.LeaderID, before.CommitIndex,
		after.Term, after.LeaderID, after.CommitIndex,
		after.AppliedIndex, 0,
	)

	cliDistributedRead(
		t, cluster, cluster.endpoints(),
		key, "preserved", true, false,
	)

	for _, node := range cluster.nodes {
		cliDistributedRead(
			t, cluster, node.grpcAddr,
			key, "preserved", true, true,
		)
	}

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		"complete-outage-client", "after-restart",
		cliPut, key, true, "updated",
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, after, 1,
	)

	cliDistributedRead(
		t, cluster, cluster.endpoints(),
		key, "updated", true, false,
	)

	t.Log("Verified full-cluster restart")
	t.Log("Verified committed data preservation")
	t.Log("Verified post-recovery mutation availability")
}
