package main

import (
	"net/http"
	"testing"
	"time"
)

func gatewayDistributedRequireValue(
	t *testing.T,
	cluster *gatewayDistributedCluster,
	path string,
	expected string,
) {
	t.Helper()

	result := cluster.request(
		t,
		http.MethodGet,
		path,
		nil,
		http.StatusOK,
	)

	if !result.Found || result.Value != expected {
		t.Fatalf(
			"Unexpected value: %+v, expected=%q",
			result,
			expected,
		)
	}
}

func gatewayDistributedRequireStatus(
	t *testing.T,
	cluster *gatewayDistributedCluster,
	node *gatewayDistributedNode,
) gatewayDistributedStatus {
	t.Helper()

	status, err := cluster.status(node)
	if err != nil {
		t.Fatal(err)
	}

	return status
}

func gatewayDistributedWaitReplication(
	t *testing.T,
	cluster *gatewayDistributedCluster,
	target uint64,
) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		ready := true

		for _, node := range cluster.nodes {
			if node.process == nil {
				continue
			}

			status, err := cluster.status(node)

			if err != nil ||
				status.CommitIndex < target ||
				status.AppliedIndex < target {
				ready = false
				break
			}
		}

		if ready {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf(
		"Replicas did not apply index %d",
		target,
	)
}

func gatewayDistributedRequireUnchangedIndexes(
	t *testing.T,
	before gatewayDistributedStatus,
	after gatewayDistributedStatus,
) {
	t.Helper()

	if before.Term != after.Term ||
		before.CommitIndex != after.CommitIndex ||
		before.AppliedIndex != after.AppliedIndex {
		t.Fatalf(
			"Unexpected Raft progress: before=%+v after=%+v",
			before,
			after,
		)
	}
}

func TestGatewayDistributedLeaderFailover(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const path = "/kv/failover-key"

	oldLeader := cluster.waitLeader(t, 0)

	result := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			"failover-initial",
			map[string]string{"value": "100"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "failover-initial", true,
	)

	gatewayDistributedRequireValue(
		t, cluster, path, "100",
	)

	before := gatewayDistributedRequireStatus(
		t, cluster, oldLeader,
	)

	gatewayDistributedWaitReplication(
		t, cluster, before.CommitIndex,
	)

	oldLeader.stop(t)

	newLeader := cluster.waitLeader(
		t, oldLeader.id,
	)

	after := gatewayDistributedRequireStatus(
		t, cluster, newLeader,
	)

	if newLeader.id == oldLeader.id ||
		after.Term <= before.Term {
		t.Fatalf(
			"Invalid leader replacement: before=%+v after=%+v",
			before,
			after,
		)
	}

	gatewayDistributedRequireValue(
		t, cluster, path, "100",
	)

	result = cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			"failover-update",
			map[string]string{"value": "200"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "failover-update", true,
	)

	gatewayDistributedRequireValue(
		t, cluster, path, "200",
	)

	t.Logf(
		"Leader failover %d -> %d; term %d -> %d",
		oldLeader.id,
		newLeader.id,
		before.Term,
		after.Term,
	)

	t.Log("Gateway recovered without restart or reconfiguration")
}

func TestGatewayDistributedDurableDeduplication(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const path = "/kv/dedup-key"
	const requestID = "dedup-original"

	leader := cluster.waitLeader(t, 0)

	body := gatewayDistributedMutation(
		requestID,
		map[string]string{"value": "100"},
	)

	original := cluster.request(
		t,
		http.MethodPut,
		path,
		body,
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, original, requestID, true,
	)

	gatewayDistributedRequireValue(
		t, cluster, path, "100",
	)

	before := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedWaitReplication(
		t, cluster, before.CommitIndex,
	)

	for i := 0; i < 10; i++ {
		replay := cluster.request(
			t,
			http.MethodPut,
			path,
			body,
			http.StatusOK,
		)

		gatewayDistributedRequireMutation(
			t, replay, requestID, true,
		)

		if replay != original {
			t.Fatalf(
				"Replay changed original response: %+v",
				replay,
			)
		}
	}

	after := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, before, after,
	)

	t.Logf(
		"10 duplicate mutations: commit index %d -> %d",
		before.CommitIndex,
		after.CommitIndex,
	)

	leader.stop(t)

	replacement := cluster.waitLeader(
		t, leader.id,
	)

	gatewayDistributedRequireValue(
		t, cluster, path, "100",
	)

	before = gatewayDistributedRequireStatus(
		t, cluster, replacement,
	)

	replay := cluster.request(
		t,
		http.MethodPut,
		path,
		body,
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, replay, requestID, true,
	)

	if replay != original {
		t.Fatalf(
			"Cross-leader replay changed response: %+v",
			replay,
		)
	}

	after = gatewayDistributedRequireStatus(
		t, cluster, replacement,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, before, after,
	)

	t.Logf(
		"Cross-leader replay: commit index %d -> %d",
		before.CommitIndex,
		after.CommitIndex,
	)

	result := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			"dedup-new-update",
			map[string]string{"value": "200"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "dedup-new-update", true,
	)

	before = gatewayDistributedRequireStatus(
		t, cluster, replacement,
	)

	replay = cluster.request(
		t,
		http.MethodPut,
		path,
		body,
		http.StatusOK,
	)

	if replay != original {
		t.Fatal("Historical replay did not preserve original result")
	}

	after = gatewayDistributedRequireStatus(
		t, cluster, replacement,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, before, after,
	)

	gatewayDistributedRequireValue(
		t, cluster, path, "200",
	)

	t.Log("Historical replay preserved newer stored value")
	t.Log("Durable request deduplication across leadership change passed")
}
