package main

import (
	"strings"
	"testing"
)

func cliDistributedRequireReplay(
	t *testing.T,
	original cliMutationResult,
	replayed cliMutationResult,
) {
	t.Helper()

	if original != replayed {
		t.Fatalf(
			"Replay response changed: original=%+v replayed=%+v",
			original,
			replayed,
		)
	}

	t.Logf(
		"Cached response preserved: operation=%s request=%s applied=%v value=%q",
		replayed.Operation,
		replayed.RequestID,
		replayed.Applied,
		replayed.Value,
	)
}

func cliDistributedRequireConflict(
	t *testing.T,
	cluster *cliDistributedCluster,
	endpoints string,
	clientID string,
	requestID string,
	args ...string,
) {
	t.Helper()

	commandArgs := []string{
		"--grpc-endpoints", endpoints,
		"--timeout", "11s",
		"--output", "json",
		"--client-id", clientID,
		"--request-id", requestID,
	}

	commandArgs = append(commandArgs, args...)

	result := cluster.execute(t, commandArgs...)

	cliDistributedRequireExit(
		t, result, cliExitFailure,
	)

	if result.Stdout != "" {
		t.Fatalf(
			"Conflict unexpectedly returned stdout: %q",
			result.Stdout,
		)
	}

	if !strings.Contains(
		strings.ToLower(result.Stderr),
		"conflict",
	) {
		t.Fatalf(
			"Expected request conflict: %q",
			result.Stderr,
		)
	}

	if !strings.Contains(result.Stderr, clientID) ||
		!strings.Contains(result.Stderr, requestID) {
		t.Fatalf(
			"Conflict lost request identity: %q",
			result.Stderr,
		)
	}

	t.Logf(
		"Conflicting request rejected: args=%q stderr=%q",
		args,
		result.Stderr,
	)
}

func TestCLIDistributedDedupPut(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const key = "dedup-put"
	const clientID = "dedup-put-client"

	before := cluster.waitConvergence(t, 1)

	original := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "original",
		cliPut, key, true, "100",
	)

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 1,
	)

	for _, node := range cluster.nodes {
		replayed := cliDistributedMutate(
			t, cluster, node.grpcAddr,
			clientID, "original",
			cliPut, key, true, "100",
		)

		cliDistributedRequireReplay(
			t, original, replayed,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "update",
		cliPut, key, true, "200",
	)

	updated := cliDistributedRequireCommitAdvance(
		t, cluster, committed, 1,
	)

	replayed := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "original",
		cliPut, key, true, "100",
	)

	cliDistributedRequireReplay(
		t, original, replayed,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "200", true, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, updated, 0,
	)

	t.Log("PUT replay preserved the newer stored value")
}

func TestCLIDistributedDedupDelete(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const key = "dedup-delete"
	const clientID = "dedup-delete-client"

	before := cluster.waitConvergence(t, 1)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "seed",
		cliPut, key, true, "100",
	)

	original := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "delete",
		cliDelete, key, true,
	)

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 2,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "", false, false,
	)

	for _, node := range cluster.nodes {
		replayed := cliDistributedMutate(
			t, cluster, node.grpcAddr,
			clientID, "delete",
			cliDelete, key, true,
		)

		cliDistributedRequireReplay(
			t, original, replayed,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "recreate",
		cliPut, key, true, "recreated",
	)

	recreated := cliDistributedRequireCommitAdvance(
		t, cluster, committed, 1,
	)

	replayed := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "delete",
		cliDelete, key, true,
	)

	cliDistributedRequireReplay(
		t, original, replayed,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "recreated", true, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, recreated, 0,
	)

	t.Log("DELETE replay did not remove the recreated key")
}

func TestCLIDistributedDedupCASMatch(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const key = "dedup-cas-match"
	const clientID = "dedup-cas-match-client"

	before := cluster.waitConvergence(t, 1)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "seed",
		cliPut, key, true, "100",
	)

	original := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas",
		cliCAS, key, true, "100", "200",
	)

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 2,
	)

	for _, node := range cluster.nodes {
		replayed := cliDistributedMutate(
			t, cluster, node.grpcAddr,
			clientID, "cas",
			cliCAS, key, true, "100", "200",
		)

		cliDistributedRequireReplay(
			t, original, replayed,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "update",
		cliPut, key, true, "300",
	)

	updated := cliDistributedRequireCommitAdvance(
		t, cluster, committed, 1,
	)

	replayed := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas",
		cliCAS, key, true, "100", "200",
	)

	cliDistributedRequireReplay(
		t, original, replayed,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "300", true, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, updated, 0,
	)

	t.Log("Successful CAS replay returned its original result")
}

func TestCLIDistributedDedupCASMismatch(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const key = "dedup-cas-mismatch"
	const clientID = "dedup-cas-mismatch-client"

	before := cluster.waitConvergence(t, 1)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "seed",
		cliPut, key, true, "100",
	)

	original := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas",
		cliCAS, key, false, "999", "200",
	)

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 2,
	)

	for _, node := range cluster.nodes {
		replayed := cliDistributedMutate(
			t, cluster, node.grpcAddr,
			clientID, "cas",
			cliCAS, key, false, "999", "200",
		)

		cliDistributedRequireReplay(
			t, original, replayed,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "change-to-expected",
		cliPut, key, true, "999",
	)

	updated := cliDistributedRequireCommitAdvance(
		t, cluster, committed, 1,
	)

	replayed := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas",
		cliCAS, key, false, "999", "200",
	)

	cliDistributedRequireReplay(
		t, original, replayed,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "999", true, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, updated, 0,
	)

	t.Log("Cached CAS mismatch was not re-evaluated")
}

func TestCLIDistributedDedupConflicts(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const clientID = "dedup-conflict-client"
	const key = "conflict-key"

	before := cluster.waitConvergence(t, 1)

	original := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "put",
		cliPut, key, true, "100",
	)

	cas := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas",
		cliCAS, key, true, "100", "200",
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "seed-delete",
		cliPut, "delete-key", true, "remove",
	)

	deleted := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "delete",
		cliDelete, "delete-key", true,
	)

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 4,
	)

	cases := []struct {
		name      string
		requestID string
		args      []string
	}{
		{
			"ChangedPutValue", "put",
			[]string{"put", key, "999"},
		},
		{
			"ChangedPutKey", "put",
			[]string{"put", "other-key", "100"},
		},
		{
			"PutToDelete", "put",
			[]string{"delete", key},
		},
		{
			"PutToCAS", "put",
			[]string{"cas", key, "200", "999"},
		},
		{
			"ChangedCASExpected", "cas",
			[]string{"cas", key, "999", "200"},
		},
		{
			"ChangedCASValue", "cas",
			[]string{"cas", key, "100", "999"},
		},
		{
			"ChangedCASKey", "cas",
			[]string{"cas", "other-key", "100", "200"},
		},
		{
			"CASToPut", "cas",
			[]string{"put", key, "999"},
		},
		{
			"ChangedDeleteKey", "delete",
			[]string{"delete", key},
		},
		{
			"DeleteToPut", "delete",
			[]string{"put", "delete-key", "999"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cliDistributedRequireConflict(
				t, cluster, endpoints,
				clientID, tc.requestID,
				tc.args...,
			)
		})
	}

	cliDistributedRequireReplay(
		t, original,
		cliDistributedMutate(
			t, cluster, endpoints,
			clientID, "put",
			cliPut, key, true, "100",
		),
	)

	cliDistributedRequireReplay(
		t, cas,
		cliDistributedMutate(
			t, cluster, endpoints,
			clientID, "cas",
			cliCAS, key, true, "100", "200",
		),
	)

	cliDistributedRequireReplay(
		t, deleted,
		cliDistributedMutate(
			t, cluster, endpoints,
			clientID, "delete",
			cliDelete, "delete-key", true,
		),
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "200", true, false,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"other-key", "", false, false,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"delete-key", "", false, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	t.Logf(
		"Rejected %d conflicting payloads without new commits",
		len(cases),
	)
}

func TestCLIDistributedDedupIdentityIsolation(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const key = "identity-isolation"
	const requestID = "shared-request"

	before := cluster.waitConvergence(t, 1)

	first := cliDistributedMutate(
		t, cluster, endpoints,
		"client-a", requestID,
		cliPut, key, true, "100",
	)

	second := cliDistributedMutate(
		t, cluster, endpoints,
		"client-b", requestID,
		cliPut, key, true, "200",
	)

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 2,
	)

	cliDistributedRequireReplay(
		t, first,
		cliDistributedMutate(
			t, cluster, endpoints,
			"client-a", requestID,
			cliPut, key, true, "100",
		),
	)

	cliDistributedRequireReplay(
		t, second,
		cliDistributedMutate(
			t, cluster, endpoints,
			"client-b", requestID,
			cliPut, key, true, "200",
		),
	)

	cliDistributedRead(
		t, cluster, endpoints,
		key, "200", true, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, committed, 0,
	)

	t.Log("Request identities were isolated by client ID")
}

func TestCLIDistributedDedupFailover(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	endpoints := cluster.endpoints()

	const clientID = "dedup-failover-client"

	before := cluster.waitConvergence(t, 1)

	put := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "put",
		cliPut, "put-key", true, "100",
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "seed-cas",
		cliPut, "cas-key", true, "100",
	)

	cas := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "cas",
		cliCAS, "cas-key", true, "100", "200",
	)

	mismatch := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "mismatch",
		cliCAS, "cas-key", false, "999", "300",
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "seed-delete",
		cliPut, "delete-key", true, "remove",
	)

	deleted := cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "delete",
		cliDelete, "delete-key", true,
	)

	committed := cliDistributedRequireCommitAdvance(
		t, cluster, before, 6,
	)

	oldLeader := cluster.waitLeader(t, 0)

	oldLeader.stop(t)
	cluster.httpClient.CloseIdleConnections()

	newLeader := cluster.waitLeader(
		t, oldLeader.id,
	)

	cliDistributedRead(
		t, cluster, newLeader.grpcAddr,
		"cas-key", "200", true, false,
	)

	baseline, err := cluster.status(newLeader)
	if err != nil {
		t.Fatal(err)
	}

	if baseline.Term <= committed.Term ||
		baseline.CommitIndex < committed.CommitIndex {
		t.Fatalf(
			"Invalid failover baseline: %+v",
			baseline,
		)
	}

	replayAll := func(endpoint string) {
		t.Helper()

		cliDistributedRequireReplay(
			t, put,
			cliDistributedMutate(
				t, cluster, endpoint,
				clientID, "put",
				cliPut, "put-key", true, "100",
			),
		)

		cliDistributedRequireReplay(
			t, cas,
			cliDistributedMutate(
				t, cluster, endpoint,
				clientID, "cas",
				cliCAS, "cas-key", true, "100", "200",
			),
		)

		cliDistributedRequireReplay(
			t, mismatch,
			cliDistributedMutate(
				t, cluster, endpoint,
				clientID, "mismatch",
				cliCAS, "cas-key", false, "999", "300",
			),
		)

		cliDistributedRequireReplay(
			t, deleted,
			cliDistributedMutate(
				t, cluster, endpoint,
				clientID, "delete",
				cliDelete, "delete-key", true,
			),
		)

		cliDistributedRequireConflict(
			t, cluster, endpoint,
			clientID, "put",
			"put", "put-key", "999",
		)

		cliDistributedRead(
			t, cluster, endpoint,
			"put-key", "100", true, false,
		)

		cliDistributedRead(
			t, cluster, endpoint,
			"cas-key", "200", true, false,
		)

		cliDistributedRead(
			t, cluster, endpoint,
			"delete-key", "", false, false,
		)
	}

	replayAll(newLeader.grpcAddr)

	after, err := cluster.status(newLeader)
	if err != nil {
		t.Fatal(err)
	}

	if after.Term != baseline.Term ||
		after.LeaderID != baseline.LeaderID ||
		after.CommitIndex != baseline.CommitIndex ||
		after.AppliedIndex != after.CommitIndex {
		t.Fatalf(
			"Replay changed Raft status: before=%+v after=%+v",
			baseline, after,
		)
	}

	oldLeader.start(t, cluster.daemonBinary)
	cluster.waitHTTP(t, oldLeader)

	recovered := cluster.waitConvergence(
		t, after.CommitIndex,
	)

	cliDistributedRequireRecoveryProgress(
		t,
		after.Term, after.LeaderID, after.CommitIndex,
		recovered.Term, recovered.LeaderID,
		recovered.CommitIndex, recovered.AppliedIndex,
		0,
	)

	replayAll(oldLeader.grpcAddr)

	for _, node := range cluster.nodes {
		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"put-key", "100", true, true,
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"cas-key", "200", true, true,
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"delete-key", "", false, true,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, recovered, 0,
	)

	t.Log("All four cached mutation results survived failover")
	t.Log("Request conflicts remained rejected")
	t.Log("Recovered cluster preserved deduplication behavior")
}
