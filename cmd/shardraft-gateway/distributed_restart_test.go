package main

import (
	"net/http"
	"testing"
)

func TestGatewayDistributedGatewayRestart(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const putPath = "/kv/restart-put"
	const casPath = "/kv/restart-cas"
	const deletePath = "/kv/restart-delete"
	const preservedPath = "/kv/restart-preserved"

	seeds := []struct {
		path  string
		id    string
		value string
	}{
		{putPath, "restart-put-seed", "100"},
		{casPath, "restart-cas-seed", "100"},
		{deletePath, "restart-delete-seed", "remove-me"},
		{preservedPath, "restart-preserved-seed", "stable"},
	}

	for _, seed := range seeds {
		response := cluster.request(
			t,
			http.MethodPut,
			seed.path,
			gatewayDistributedMutation(
				seed.id,
				map[string]string{"value": seed.value},
			),
			http.StatusOK,
		)

		gatewayDistributedRequireMutation(
			t, response, seed.id, true,
		)

		gatewayDistributedRequireValue(
			t, cluster, seed.path, seed.value,
		)
	}

	leader := cluster.waitLeader(t, 0)

	before := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedWaitConvergence(
		t, cluster, before.CommitIndex,
	)

	backendPIDs := make(map[int]int)

	for _, node := range cluster.nodes {
		backendPIDs[node.id] = node.process.Process.Pid
	}

	oldGatewayPID := cluster.gateway.process.Process.Pid

	t.Logf(
		"Before gateway restart: PID=%d leader=%d term=%d commit=%d",
		oldGatewayPID,
		leader.id,
		before.Term,
		before.CommitIndex,
	)

	cluster.gateway.stop(t)
	cluster.httpClient.CloseIdleConnections()

	if cluster.gateway.process != nil {
		t.Fatal("Gateway process did not stop")
	}

	if t.Failed() {
		t.FailNow()
	}

	for _, node := range cluster.nodes {
		if node.process == nil ||
			node.process.Process.Pid != backendPIDs[node.id] {
			t.Fatalf("Backend process changed: node=%d", node.id)
		}

		status := gatewayDistributedRequireStatus(
			t, cluster, node,
		)

		if status.Term != before.Term ||
			status.LeaderID != before.ID ||
			status.CommitIndex != before.CommitIndex ||
			status.AppliedIndex != before.CommitIndex {
			t.Fatalf(
				"Backend state changed during gateway shutdown: %+v",
				status,
			)
		}
	}

	t.Log("Gateway stopped; all three backend replicas remain healthy")

	cluster.gateway.start(t, cluster.gatewayBinary)

	gatewayDistributedWaitHTTP(
		t,
		cluster.gateway.httpAddr,
		"/nonexistent",
		http.StatusNotFound,
	)

	newGatewayPID := cluster.gateway.process.Process.Pid

	if newGatewayPID == oldGatewayPID {
		t.Fatal("Expected a new gateway process")
	}

	t.Logf(
		"Gateway restarted: old_PID=%d new_PID=%d",
		oldGatewayPID,
		newGatewayPID,
	)

	gatewayDistributedWaitMajorityReady(
		t, cluster, preservedPath, "stable",
	)

	for _, seed := range seeds {
		for _, mode := range []string{
			"",
			"?read_mode=linearizable",
			"?read_mode=local",
		} {
			gatewayDistributedRequireValue(
				t,
				cluster,
				seed.path+mode,
				seed.value,
			)
		}
	}

	t.Log("All four committed keys survived gateway restart")
	t.Log("Verified 12 reads across default, linearizable and local modes")

	afterRestart := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, before, afterRestart,
	)

	t.Log("Gateway restart caused zero additional Raft commits")

	put := cluster.request(
		t,
		http.MethodPut,
		putPath,
		gatewayDistributedMutation(
			"restart-put-update",
			map[string]string{"value": "200"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, put, "restart-put-update", true,
	)

	if put.Value != "200" {
		t.Fatalf("Unexpected PUT result: %+v", put)
	}

	cas := cluster.request(
		t,
		http.MethodPost,
		casPath+"/cas",
		gatewayDistributedMutation(
			"restart-cas-update",
			map[string]string{
				"expected": "100",
				"value":    "200",
			},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, cas, "restart-cas-update", true,
	)

	if cas.Value != "200" {
		t.Fatalf("Unexpected CAS result: %+v", cas)
	}

	deleted := cluster.request(
		t,
		http.MethodDelete,
		deletePath,
		gatewayDistributedMutation(
			"restart-delete-update",
			nil,
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, deleted, "restart-delete-update", true,
	)

	gatewayDistributedRequireValue(
		t, cluster, putPath, "200",
	)

	gatewayDistributedRequireValue(
		t, cluster, casPath, "200",
	)

	missing := cluster.request(
		t,
		http.MethodGet,
		deletePath,
		nil,
		http.StatusNotFound,
	)

	if missing.Found ||
		missing.Value != "" ||
		missing.Code != "key_not_found" {
		t.Fatalf("Unexpected deleted key response: %+v", missing)
	}

	gatewayDistributedRequireValue(
		t, cluster, preservedPath, "stable",
	)

	final := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireCommitDelta(
		t, afterRestart, final, 3,
	)

	gatewayDistributedWaitConvergence(
		t, cluster, final.CommitIndex,
	)

	gatewayDistributedVerifyReplicaData(
		t,
		cluster,
		[]gatewayReplicaExpectedValue{
			{key: "restart-put", value: "200", found: true},
			{key: "restart-cas", value: "200", found: true},
			{key: "restart-delete", found: false},
			{key: "restart-preserved", value: "stable", found: true},
		},
	)

	for _, node := range cluster.nodes {
		if node.process == nil ||
			node.process.Process.Pid != backendPIDs[node.id] {
			t.Fatalf(
				"Backend process changed during gateway restart: node=%d",
				node.id,
			)
		}

		t.Logf(
			"Backend replica %d PID=%d remained unchanged",
			node.id,
			backendPIDs[node.id],
		)
	}

	if cluster.gateway.process == nil ||
		cluster.gateway.process.Process.Pid != newGatewayPID {
		t.Fatal("Restarted gateway process changed unexpectedly")
	}

	t.Log("Post-restart mutations: PUT=1 CAS=1 DELETE=1")
	t.Log("All three replicas contain the expected final data")
	t.Log("PASS: Gateway restart and data persistence")
}
