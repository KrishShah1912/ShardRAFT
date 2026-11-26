package main

import (
	"fmt"
	"net/http"
	"testing"
)

func TestGatewayDistributedConcurrentCASContention(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const workers = gatewayDistributedConcurrentWorkers
	const path = "/kv/concurrent-cas-key"
	const initialValue = "100"

	seed := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			"concurrent-cas-seed",
			map[string]string{"value": initialValue},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t,
		seed,
		"concurrent-cas-seed",
		true,
	)

	gatewayDistributedRequireValue(
		t,
		cluster,
		path,
		initialValue,
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

	requests := make(
		[]gatewayDistributedConcurrentRequest,
		workers,
	)

	values := make([]string, workers)
	requestIDs := make([]string, workers)

	for i := range requests {
		values[i] = fmt.Sprintf("%d", 201+i)
		requestIDs[i] = fmt.Sprintf("concurrent-cas-%02d", i)

		requests[i] = gatewayDistributedConcurrentRequest{
			method: http.MethodPost,
			path:   path + "/cas",
			body: gatewayDistributedMutation(
				requestIDs[i],
				map[string]string{
					"expected": initialValue,
					"value":    values[i],
				},
			),
		}
	}

	results := gatewayDistributedRunConcurrent(
		t,
		cluster,
		requests,
	)

	applied := 0
	notApplied := 0
	winner := -1

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
			result.response.Applied,
		)

		if result.response.Applied {
			applied++
			winner = i

			if result.response.Value != values[i] {
				t.Fatalf(
					"Winner value=%q want=%q",
					result.response.Value,
					values[i],
				)
			}
		} else {
			notApplied++
		}
	}

	if applied != 1 ||
		notApplied != workers-1 ||
		winner < 0 {
		t.Fatalf(
			"Unexpected CAS outcomes: applied=%d not_applied=%d winner=%d",
			applied,
			notApplied,
			winner,
		)
	}

	winningValue := values[winner]

	t.Logf(
		"CAS contention: workers=%d applied=%d not_applied=%d winner=%d value=%q",
		workers,
		applied,
		notApplied,
		winner,
		winningValue,
	)

	gatewayDistributedRequireValue(
		t,
		cluster,
		path,
		winningValue,
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
				"CAS replay %d changed result: got=%+v want=%+v",
				i,
				replay,
				results[i].response,
			)
		}
	}

	gatewayDistributedRequireValue(
		t,
		cluster,
		path,
		winningValue,
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
		"CAS replay verification: replays=%d additional_commits=0",
		workers,
	)

	t.Log(
		"PASS: Concurrent CAS contention",
	)
}
