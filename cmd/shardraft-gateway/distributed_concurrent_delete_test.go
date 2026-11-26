package main

import (
	"fmt"
	"net/http"
	"testing"
)

func TestGatewayDistributedConcurrentDelete(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const workers = gatewayDistributedConcurrentWorkers

	requests := make(
		[]gatewayDistributedConcurrentRequest,
		workers,
	)

	requestIDs := make([]string, workers)

	for i := range requests {
		path := fmt.Sprintf(
			"/kv/concurrent-delete-%02d",
			i,
		)

		seedID := fmt.Sprintf(
			"concurrent-delete-seed-%02d",
			i,
		)

		value := fmt.Sprintf("value-%02d", i)

		seed := cluster.request(
			t,
			http.MethodPut,
			path,
			gatewayDistributedMutation(
				seedID,
				map[string]string{
					"value": value,
				},
			),
			http.StatusOK,
		)

		gatewayDistributedRequireMutation(
			t,
			seed,
			seedID,
			true,
		)

		gatewayDistributedRequireValue(
			t,
			cluster,
			path,
			value,
		)

		requestIDs[i] = fmt.Sprintf(
			"concurrent-delete-request-%02d",
			i,
		)

		requests[i] = gatewayDistributedConcurrentRequest{
			method: http.MethodDelete,
			path:   path,
			body: gatewayDistributedMutation(
				requestIDs[i],
				nil,
			),
		}
	}

	const preservedPath = "/kv/concurrent-delete-preserved"
	const preservedValue = "must-survive"

	preserved := cluster.request(
		t,
		http.MethodPut,
		preservedPath,
		gatewayDistributedMutation(
			"concurrent-delete-preserved-seed",
			map[string]string{
				"value": preservedValue,
			},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t,
		preserved,
		"concurrent-delete-preserved-seed",
		true,
	)

	leader := cluster.waitLeader(t, 0)

	before := gatewayDistributedRequireStatus(
		t,
		cluster,
		leader,
	)

	gatewayDistributedWaitReplication(
		t,
		cluster,
		before.CommitIndex,
	)

	t.Logf(
		"Seeded %d independent keys and one preserved key",
		workers,
	)

	results := gatewayDistributedRunConcurrent(
		t,
		cluster,
		requests,
	)

	for i, result := range results {
		if result.status != http.StatusOK {
			t.Fatalf(
				"Worker %d: HTTP=%d response=%+v",
				i,
				result.status,
				result.response,
			)
		}

		gatewayDistributedRequireMutation(
			t,
			result.response,
			requestIDs[i],
			true,
		)
	}

	t.Logf(
		"Concurrent DELETE: workers=%d successful=%d",
		workers,
		len(results),
	)

	for _, request := range requests {
		response := cluster.request(
			t,
			http.MethodGet,
			request.path,
			nil,
			http.StatusNotFound,
		)

		if response.Found ||
			response.Value != "" ||
			response.Code != "key_not_found" ||
			response.Error == "" {
			t.Fatalf(
				"Deleted key remains visible: path=%s response=%+v",
				request.path,
				response,
			)
		}
	}

	gatewayDistributedRequireValue(
		t,
		cluster,
		preservedPath,
		preservedValue,
	)

	after := gatewayDistributedRequireStatus(
		t,
		cluster,
		leader,
	)

	gatewayDistributedRequireCommitDelta(
		t,
		before,
		after,
		uint64(workers),
	)

	statuses := gatewayDistributedWaitConvergence(
		t,
		cluster,
		after.CommitIndex,
	)

	for _, status := range statuses {
		if status.Term != before.Term ||
			status.LeaderID != before.ID ||
			status.CommitIndex != after.CommitIndex ||
			status.AppliedIndex != status.CommitIndex {
			t.Fatalf(
				"Replica %d has unexpected state: %+v",
				status.ID,
				status,
			)
		}

		t.Logf(
			"Replica %d synchronized: commit=%d applied=%d",
			status.ID,
			status.CommitIndex,
			status.AppliedIndex,
		)
	}

	for i, request := range requests {
		replay := cluster.request(
			t,
			request.method,
			request.path,
			request.body,
			http.StatusOK,
		)

		if replay != results[i].response {
			t.Fatalf(
				"DELETE replay %d changed result: got=%+v want=%+v",
				i,
				replay,
				results[i].response,
			)
		}
	}

	for _, request := range requests {
		response := cluster.request(
			t,
			http.MethodGet,
			request.path,
			nil,
			http.StatusNotFound,
		)

		if response.Found ||
			response.Value != "" ||
			response.Code != "key_not_found" {
			t.Fatalf(
				"Deleted key visible after replay: path=%s response=%+v",
				request.path,
				response,
			)
		}
	}

	gatewayDistributedRequireValue(
		t,
		cluster,
		preservedPath,
		preservedValue,
	)

	final := gatewayDistributedRequireStatus(
		t,
		cluster,
		leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t,
		after,
		final,
	)

	t.Logf(
		"DELETE verification: missing_keys=%d preserved_keys=1 replays=%d additional_commits=0",
		workers,
		workers,
	)

	t.Log(
		"PASS: Concurrent independent DELETE operations",
	)
}
