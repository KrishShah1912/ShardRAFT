package main

import (
	"fmt"
	"net/http"
	"testing"
)

func TestGatewayDistributedConcurrentConflictingIdentities(
	t *testing.T,
) {
	cluster := newGatewayDistributedCluster(t)

	const workers = gatewayDistributedConcurrentWorkers
	const path = "/kv/concurrent-conflict-key"
	const requestID = "concurrent-conflicting-identity"

	seed := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			"concurrent-conflict-seed",
			map[string]string{"value": "initial"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t,
		seed,
		"concurrent-conflict-seed",
		true,
	)

	gatewayDistributedRequireValue(
		t,
		cluster,
		path,
		"initial",
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

	for i := range requests {
		values[i] = fmt.Sprintf("candidate-%02d", i)

		requests[i] = gatewayDistributedConcurrentRequest{
			method: http.MethodPut,
			path:   path,
			body: gatewayDistributedMutation(
				requestID,
				map[string]string{
					"value": values[i],
				},
			),
		}
	}

	results := gatewayDistributedRunConcurrent(
		t,
		cluster,
		requests,
	)

	successes := 0
	conflicts := 0
	winner := -1

	var winningResponse gatewayDistributedResponse

	for i, result := range results {
		switch result.status {
		case http.StatusOK:
			successes++
			winner = i

			gatewayDistributedRequireMutation(
				t,
				result.response,
				requestID,
				true,
			)

			if result.response.Value != values[i] {
				t.Fatalf(
					"Worker %d: value=%q want=%q",
					i,
					result.response.Value,
					values[i],
				)
			}

			winningResponse = result.response

		case http.StatusConflict:
			conflicts++

			gatewayDistributedRequireConflict(
				t,
				result.response,
				requestID,
			)

		default:
			t.Fatalf(
				"Worker %d: unexpected HTTP=%d response=%+v",
				i,
				result.status,
				result.response,
			)
		}
	}

	if successes != 1 ||
		conflicts != workers-1 ||
		winner < 0 {
		t.Fatalf(
			"Unexpected outcomes: successes=%d conflicts=%d winner=%d",
			successes,
			conflicts,
			winner,
		)
	}

	t.Logf(
		"Concurrent conflicts: workers=%d successes=%d conflicts=%d winner=%d value=%q",
		workers,
		successes,
		conflicts,
		winner,
		values[winner],
	)

	gatewayDistributedRequireValue(
		t,
		cluster,
		path,
		values[winner],
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
		1,
	)

	statuses := gatewayDistributedWaitConvergence(
		t,
		cluster,
		after.CommitIndex,
	)

	for _, status := range statuses {
		if status.Term != before.Term ||
			status.LeaderID != before.ID ||
			status.CommitIndex != before.CommitIndex+1 ||
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

	for i := 0; i < 10; i++ {
		replay := cluster.request(
			t,
			http.MethodPut,
			path,
			requests[winner].body,
			http.StatusOK,
		)

		if replay != winningResponse {
			t.Fatalf(
				"Winning replay %d changed result: got=%+v want=%+v",
				i,
				replay,
				winningResponse,
			)
		}
	}

	rejectedReplays := 0

	for i, request := range requests {
		if i == winner {
			continue
		}

		replay := cluster.request(
			t,
			http.MethodPut,
			path,
			request.body,
			http.StatusConflict,
		)

		gatewayDistributedRequireConflict(
			t,
			replay,
			requestID,
		)

		rejectedReplays++
	}

	gatewayDistributedRequireValue(
		t,
		cluster,
		path,
		values[winner],
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
		"Replay verification: winning_replays=10 rejected_replays=%d additional_commits=0",
		rejectedReplays,
	)

	t.Log(
		"PASS: Concurrent conflicting identities",
	)
}
