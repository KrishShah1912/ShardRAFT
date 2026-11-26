package main

import (
	"net/http"
	"testing"
)

func TestGatewayDistributedConcurrentIdenticalMutations(
	t *testing.T,
) {
	tests := []struct {
		name        string
		method      string
		fields      map[string]string
		applied     bool
		resultValue string
		storedValue string
		missing     bool
	}{
		{
			name:   "Put",
			method: http.MethodPut,
			fields: map[string]string{
				"value": "200",
			},
			applied:     true,
			resultValue: "200",
			storedValue: "200",
		},
		{
			name:    "Delete",
			method:  http.MethodDelete,
			applied: true,
			missing: true,
		},
		{
			name:   "CASMatch",
			method: http.MethodPost,
			fields: map[string]string{
				"expected": "100",
				"value":    "200",
			},
			applied:     true,
			resultValue: "200",
			storedValue: "200",
		},
		{
			name:   "CASMismatch",
			method: http.MethodPost,
			fields: map[string]string{
				"expected": "999",
				"value":    "200",
			},
			applied:     false,
			storedValue: "100",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newGatewayDistributedCluster(t)

			const workers = gatewayDistributedConcurrentWorkers
			const path = "/kv/concurrent-dedup-key"
			const requestID = "concurrent-identical-mutation"

			seed := cluster.request(
				t,
				http.MethodPut,
				path,
				gatewayDistributedMutation(
					"concurrent-dedup-seed",
					map[string]string{
						"value": "100",
					},
				),
				http.StatusOK,
			)

			gatewayDistributedRequireMutation(
				t,
				seed,
				"concurrent-dedup-seed",
				true,
			)

			gatewayDistributedRequireValue(
				t,
				cluster,
				path,
				"100",
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

			mutationPath := path

			if tc.method == http.MethodPost {
				mutationPath += "/cas"
			}

			body := gatewayDistributedMutation(
				requestID,
				tc.fields,
			)

			requests := make(
				[]gatewayDistributedConcurrentRequest,
				workers,
			)

			for i := range requests {
				requests[i] = gatewayDistributedConcurrentRequest{
					method: tc.method,
					path:   mutationPath,
					body:   body,
				}
			}

			results := gatewayDistributedRunConcurrent(
				t,
				cluster,
				requests,
			)

			original := results[0].response

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
					requestID,
					tc.applied,
				)

				if result.response.Value != tc.resultValue {
					t.Fatalf(
						"Worker %d: value=%q want=%q",
						i,
						result.response.Value,
						tc.resultValue,
					)
				}

				if result.response != original {
					t.Fatalf(
						"Worker %d received a different result: got=%+v want=%+v",
						i,
						result.response,
						original,
					)
				}
			}

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

			if tc.missing {
				result := cluster.request(
					t,
					http.MethodGet,
					path,
					nil,
					http.StatusNotFound,
				)

				if result.Found ||
					result.Code != "key_not_found" {
					t.Fatalf(
						"Deleted key remains visible: %+v",
						result,
					)
				}
			} else {
				gatewayDistributedRequireValue(
					t,
					cluster,
					path,
					tc.storedValue,
				)
			}

			for i := 0; i < 10; i++ {
				replay := cluster.request(
					t,
					tc.method,
					mutationPath,
					body,
					http.StatusOK,
				)

				if replay != original {
					t.Fatalf(
						"Replay %d changed result: %+v",
						i,
						replay,
					)
				}
			}

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
				"Concurrent identical %s: workers=%d successful=%d commit_delta=1 replays=10 additional_commits=0",
				tc.name,
				workers,
				len(results),
			)
		})
	}
}
