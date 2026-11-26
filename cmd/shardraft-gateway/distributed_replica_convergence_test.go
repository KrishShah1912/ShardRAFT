package main

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type gatewayReplicaExpectedValue struct {
	key   string
	value string
	found bool
}

func gatewayDistributedVerifyReplicaData(
	t *testing.T,
	cluster *gatewayDistributedCluster,
	expected []gatewayReplicaExpectedValue,
) {
	t.Helper()

	for _, node := range cluster.nodes {
		t.Run(fmt.Sprintf("Replica%d", node.id), func(t *testing.T) {
			conn, err := grpc.NewClient(
				node.grpcAddr,
				grpc.WithTransportCredentials(
					insecure.NewCredentials(),
				),
				grpc.WithDisableRetry(),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			client := shardraftpb.NewKVServiceClient(conn)

			present := 0
			missing := 0

			for _, item := range expected {
				ctx, cancel := context.WithTimeout(
					context.Background(),
					3*time.Second,
				)

				response, err := client.Get(
					ctx,
					&shardraftpb.GetRequest{
						Header: &shardraftpb.RequestHeader{
							ShardId: 1,
							Epoch:   1,
						},
						Key: item.key,
						ReadMode: shardraftpb.
							ReadMode_READ_MODE_LOCAL,
					},
				)

				cancel()

				if err != nil {
					t.Fatalf(
						"Replica %d key=%q RPC error: %v",
						node.id,
						item.key,
						err,
					)
				}

				if response == nil {
					t.Fatal("Missing gRPC response")
				}

				if response.GetKey() != item.key {
					t.Fatalf(
						"Replica %d: key=%q want=%q",
						node.id,
						response.GetKey(),
						item.key,
					)
				}

				if item.found {
					if !response.GetFound() ||
						response.GetValue() != item.value ||
						response.GetError().GetCode() !=
							shardraftpb.ErrorCode_ERROR_CODE_OK {
						t.Fatalf(
							"Replica %d key=%q want=%q response=%+v",
							node.id,
							item.key,
							item.value,
							response,
						)
					}

					present++
				} else {
					if response.GetFound() ||
						response.GetValue() != "" ||
						response.GetError().GetCode() !=
							shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND {
						t.Fatalf(
							"Replica %d deleted key=%q response=%+v",
							node.id,
							item.key,
							response,
						)
					}

					missing++
				}
			}

			t.Logf(
				"Replica %d direct LOCAL reads: present=%d missing=%d total=%d",
				node.id,
				present,
				missing,
				len(expected),
			)
		})
	}

	if t.Failed() {
		t.FailNow()
	}
}

func TestGatewayDistributedConcurrentReplicaConvergence(
	t *testing.T,
) {
	cluster := newGatewayDistributedCluster(t)

	const workers = gatewayDistributedConcurrentWorkers

	expected := make(
		[]gatewayReplicaExpectedValue,
		0,
		workers*3+1,
	)

	requests := make(
		[]gatewayDistributedConcurrentRequest,
		0,
		workers*3,
	)

	requestIDs := make([]string, 0, workers*3)

	seed := func(key, value string) {
		t.Helper()

		id := "replica-seed-" + key

		result := cluster.request(
			t,
			http.MethodPut,
			"/kv/"+key,
			gatewayDistributedMutation(
				id,
				map[string]string{"value": value},
			),
			http.StatusOK,
		)

		gatewayDistributedRequireMutation(
			t, result, id, true,
		)
	}

	addRequest := func(
		method string,
		path string,
		id string,
		fields map[string]string,
	) {
		requests = append(
			requests,
			gatewayDistributedConcurrentRequest{
				method: method,
				path:   path,
				body: gatewayDistributedMutation(
					id,
					fields,
				),
			},
		)

		requestIDs = append(requestIDs, id)
	}

	seed("replica-preserved", "stable")

	expected = append(
		expected,
		gatewayReplicaExpectedValue{
			key:   "replica-preserved",
			value: "stable",
			found: true,
		},
	)

	for i := 0; i < workers; i++ {
		putKey := fmt.Sprintf("replica-put-%03d", i)
		casKey := fmt.Sprintf("replica-cas-%03d", i)
		deleteKey := fmt.Sprintf("replica-delete-%03d", i)

		putValue := fmt.Sprintf("created-%03d", i)
		casValue := fmt.Sprintf("updated-%03d", i)

		seed(casKey, "initial")
		seed(deleteKey, "remove-me")

		addRequest(
			http.MethodPut,
			"/kv/"+putKey,
			"mutation-"+putKey,
			map[string]string{
				"value": putValue,
			},
		)

		addRequest(
			http.MethodPost,
			"/kv/"+casKey+"/cas",
			"mutation-"+casKey,
			map[string]string{
				"expected": "initial",
				"value":    casValue,
			},
		)

		addRequest(
			http.MethodDelete,
			"/kv/"+deleteKey,
			"mutation-"+deleteKey,
			nil,
		)

		expected = append(
			expected,
			gatewayReplicaExpectedValue{
				key:   putKey,
				value: putValue,
				found: true,
			},
			gatewayReplicaExpectedValue{
				key:   casKey,
				value: casValue,
				found: true,
			},
			gatewayReplicaExpectedValue{
				key:   deleteKey,
				found: false,
			},
		)
	}

	leader := cluster.waitLeader(t, 0)

	before := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedWaitConvergence(
		t, cluster, before.CommitIndex,
	)

	t.Logf(
		"Mixed workload prepared: PUT=%d CAS=%d DELETE=%d",
		workers,
		workers,
		workers,
	)

	results := gatewayDistributedRunConcurrent(
		t, cluster, requests,
	)

	for i, result := range results {
		if result.status != http.StatusOK {
			t.Fatalf(
				"Mutation %d: HTTP=%d response=%+v",
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

	after := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireCommitDelta(
		t,
		before,
		after,
		uint64(len(requests)),
	)

	statuses := gatewayDistributedWaitConvergence(
		t, cluster, after.CommitIndex,
	)

	if len(statuses) != len(cluster.nodes) {
		t.Fatal("Incomplete replica convergence")
	}

	for _, status := range statuses {
		if status.Term != before.Term ||
			status.LeaderID != before.ID ||
			status.CommitIndex != after.CommitIndex ||
			status.AppliedIndex != after.CommitIndex {
			t.Fatalf(
				"Unexpected replica status: %+v",
				status,
			)
		}
	}

	t.Run("BeforeReplay", func(t *testing.T) {
		gatewayDistributedVerifyReplicaData(
			t, cluster, expected,
		)
	})

	if t.Failed() {
		t.FailNow()
	}

	t.Logf(
		"Initial data verification: replicas=%d keys_per_replica=%d total_reads=%d",
		len(cluster.nodes),
		len(expected),
		len(cluster.nodes)*len(expected),
	)

	replays := gatewayDistributedRunConcurrent(
		t, cluster, requests,
	)

	for i, replay := range replays {
		if replay.status != results[i].status ||
			replay.response != results[i].response {
			t.Fatalf(
				"Replay %d changed response: got=%+v want=%+v",
				i,
				replay,
				results[i],
			)
		}
	}

	replayAfter := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, after, replayAfter,
	)

	finalStatuses := gatewayDistributedWaitConvergence(
		t, cluster, replayAfter.CommitIndex,
	)

	for _, status := range finalStatuses {
		if status.Term != before.Term ||
			status.LeaderID != before.ID ||
			status.CommitIndex != after.CommitIndex ||
			status.AppliedIndex != after.CommitIndex {
			t.Fatalf(
				"Replay changed replica state: %+v",
				status,
			)
		}
	}

	t.Run("AfterReplay", func(t *testing.T) {
		gatewayDistributedVerifyReplicaData(
			t, cluster, expected,
		)
	})

	if t.Failed() {
		t.FailNow()
	}

	t.Logf(
		"Mixed workload: mutations=%d replays=%d additional_commits=0",
		len(requests),
		len(replays),
	)

	t.Logf(
		"Final convergence: replicas=%d keys_per_replica=%d direct_reads=%d",
		len(cluster.nodes),
		len(expected),
		2*len(cluster.nodes)*len(expected),
	)

	t.Log("PASS: Mixed concurrent workload and direct replica data convergence")
}
