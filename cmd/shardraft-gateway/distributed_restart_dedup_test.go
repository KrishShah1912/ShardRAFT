package main

import (
	"net/http"
	"testing"
)

func TestGatewayDistributedDeduplicationAfterGatewayRestart(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const putPath = "/kv/restart-dedup-put"
	const casPath = "/kv/restart-dedup-cas"
	const deletePath = "/kv/restart-dedup-delete"
	const mismatchPath = "/kv/restart-dedup-mismatch"
	const preservedPath = "/kv/restart-dedup-preserved"

	seeds := []struct {
		path  string
		id    string
		value string
	}{
		{putPath, "restart-dedup-put-seed", "100"},
		{casPath, "restart-dedup-cas-seed", "100"},
		{deletePath, "restart-dedup-delete-seed", "100"},
		{mismatchPath, "restart-dedup-mismatch-seed", "100"},
		{preservedPath, "restart-dedup-preserved-seed", "stable"},
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
	}

	type mutationCase struct {
		name    string
		method  string
		path    string
		id      string
		fields  map[string]string
		applied bool
		value   string
	}

	cases := []mutationCase{
		{
			name:    "PUT",
			method:  http.MethodPut,
			path:    putPath,
			id:      "restart-dedup-put-original",
			fields:  map[string]string{"value": "200"},
			applied: true,
			value:   "200",
		},
		{
			name:   "CASMatch",
			method: http.MethodPost,
			path:   casPath + "/cas",
			id:     "restart-dedup-cas-original",
			fields: map[string]string{
				"expected": "100",
				"value":    "200",
			},
			applied: true,
			value:   "200",
		},
		{
			name:    "DELETE",
			method:  http.MethodDelete,
			path:    deletePath,
			id:      "restart-dedup-delete-original",
			applied: true,
		},
		{
			name:   "CASMismatch",
			method: http.MethodPost,
			path:   mismatchPath + "/cas",
			id:     "restart-dedup-mismatch-original",
			fields: map[string]string{
				"expected": "999",
				"value":    "200",
			},
			applied: false,
		},
	}

	leader := cluster.waitLeader(t, 0)

	beforeOriginal := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	originals := make([]gatewayDistributedResponse, len(cases))

	for i, tc := range cases {
		response := cluster.request(
			t,
			tc.method,
			tc.path,
			gatewayDistributedMutation(tc.id, tc.fields),
			http.StatusOK,
		)

		gatewayDistributedRequireMutation(
			t, response, tc.id, tc.applied,
		)

		if response.Value != tc.value {
			t.Fatalf(
				"%s unexpected original result: %+v",
				tc.name, response,
			)
		}

		originals[i] = response
	}

	afterOriginal := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireCommitDelta(
		t, beforeOriginal, afterOriginal, 4,
	)

	// Change the current state after recording the original results.
	// Historical replays must not overwrite these newer values.
	newer := []struct {
		path  string
		id    string
		value string
	}{
		{putPath, "restart-dedup-put-newer", "300"},
		{casPath, "restart-dedup-cas-newer", "300"},
		{deletePath, "restart-dedup-delete-newer", "recreated"},
		{mismatchPath, "restart-dedup-mismatch-newer", "999"},
	}

	for _, update := range newer {
		response := cluster.request(
			t,
			http.MethodPut,
			update.path,
			gatewayDistributedMutation(
				update.id,
				map[string]string{"value": update.value},
			),
			http.StatusOK,
		)

		gatewayDistributedRequireMutation(
			t, response, update.id, true,
		)
	}

	beforeRestart := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireCommitDelta(
		t, afterOriginal, beforeRestart, 4,
	)

	gatewayDistributedWaitConvergence(
		t, cluster, beforeRestart.CommitIndex,
	)

	backendPIDs := make(map[int]int)

	for _, node := range cluster.nodes {
		backendPIDs[node.id] = node.process.Process.Pid
	}

	oldPID := cluster.gateway.process.Process.Pid

	cluster.gateway.stop(t)
	cluster.httpClient.CloseIdleConnections()

	if t.Failed() {
		t.FailNow()
	}

	cluster.gateway.start(t, cluster.gatewayBinary)

	gatewayDistributedWaitHTTP(
		t,
		cluster.gateway.httpAddr,
		"/nonexistent",
		http.StatusNotFound,
	)

	newPID := cluster.gateway.process.Process.Pid

	if oldPID == newPID {
		t.Fatal("Gateway PID did not change")
	}

	t.Logf(
		"Gateway restarted: old_PID=%d new_PID=%d",
		oldPID, newPID,
	)

	gatewayDistributedWaitMajorityReady(
		t, cluster, preservedPath, "stable",
	)

	afterRestart := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, beforeRestart, afterRestart,
	)

	const replaysPerMutation = 10

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for replay := 0; replay < replaysPerMutation; replay++ {
				response := cluster.request(
					t,
					tc.method,
					tc.path,
					gatewayDistributedMutation(
						tc.id, tc.fields,
					),
					http.StatusOK,
				)

				if response != originals[i] {
					t.Fatalf(
						"Replay %d changed result: original=%+v replay=%+v",
						replay,
						originals[i],
						response,
					)
				}
			}

			status := gatewayDistributedRequireStatus(
				t, cluster, leader,
			)

			gatewayDistributedRequireUnchangedIndexes(
				t, afterRestart, status,
			)

			t.Logf(
				"%s: %d exact historical replays; additional_commits=0",
				tc.name,
				replaysPerMutation,
			)
		})
	}

	if t.Failed() {
		t.FailNow()
	}

	// Previously committed identities must also reject different payloads.
	for _, tc := range cases {
		response := cluster.request(
			t,
			http.MethodPut,
			putPath,
			gatewayDistributedMutation(
				tc.id,
				map[string]string{"value": "conflicting"},
			),
			http.StatusConflict,
		)

		gatewayDistributedRequireConflict(
			t, response, tc.id,
		)
	}

	t.Log("All four historical request identities rejected conflicting payloads")

	for _, update := range newer {
		gatewayDistributedRequireValue(
			t, cluster, update.path, update.value,
		)
	}

	gatewayDistributedRequireValue(
		t, cluster, preservedPath, "stable",
	)

	final := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, afterRestart, final,
	)

	gatewayDistributedWaitConvergence(
		t, cluster, final.CommitIndex,
	)

	gatewayDistributedVerifyReplicaData(
		t,
		cluster,
		[]gatewayReplicaExpectedValue{
			{key: "restart-dedup-put", value: "300", found: true},
			{key: "restart-dedup-cas", value: "300", found: true},
			{key: "restart-dedup-delete", value: "recreated", found: true},
			{key: "restart-dedup-mismatch", value: "999", found: true},
			{key: "restart-dedup-preserved", value: "stable", found: true},
		},
	)

	for _, node := range cluster.nodes {
		if node.process == nil ||
			node.process.Process.Pid != backendPIDs[node.id] {
			t.Fatalf(
				"Backend process changed: node=%d",
				node.id,
			)
		}
	}

	if cluster.gateway.process == nil ||
		cluster.gateway.process.Process.Pid != newPID {
		t.Fatal("Restarted gateway process changed unexpectedly")
	}

	t.Logf(
		"Verified historical replays=%d conflicts=%d additional_commits=0",
		len(cases)*replaysPerMutation,
		len(cases),
	)

	t.Log("Newer values preserved across all three replicas")
	t.Log("PASS: Durable deduplication across gateway restart")
}
