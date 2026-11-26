package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func gatewayDistributedQuorumRequest(
	t *testing.T,
	cluster *gatewayDistributedCluster,
	method string,
	path string,
	body any,
) (int, gatewayDistributedResponse) {
	t.Helper()

	var result gatewayDistributedResponse
	var payload []byte

	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}

	request, err := http.NewRequest(
		method,
		cluster.gatewayURL+path,
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatal(err)
	}

	if body != nil {
		request.Header.Set(
			"Content-Type",
			"application/json",
		)
	}

	response, err := cluster.httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Invalid response: %s: %v", data, err)
	}

	return response.StatusCode, result
}

func TestGatewayDistributedQuorumLossAndRecovery(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const path = "/kv/quorum-key"
	const preservedPath = "/kv/quorum-preserved-key"

	gatewayPID := cluster.gateway.process.Process.Pid

	initial := gatewayDistributedMutation(
		"quorum-initial",
		map[string]string{"value": "100"},
	)

	response := cluster.request(
		t,
		http.MethodPut,
		path,
		initial,
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, response, "quorum-initial", true,
	)

	preserved := gatewayDistributedMutation(
		"quorum-preserved",
		map[string]string{"value": "stable"},
	)

	response = cluster.request(
		t,
		http.MethodPut,
		preservedPath,
		preserved,
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, response, "quorum-preserved", true,
	)

	gatewayDistributedRequireValue(
		t, cluster, path, "100",
	)

	leader := cluster.waitLeader(t, 0)

	initialStatus := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedWaitReplication(
		t, cluster, initialStatus.CommitIndex,
	)

	t.Logf(
		"Healthy cluster: leader=%d term=%d commit=%d",
		leader.id,
		initialStatus.Term,
		initialStatus.CommitIndex,
	)

	var stopped []*gatewayDistributedNode

	for _, node := range cluster.nodes {
		if node != leader {
			node.stop(t)
			stopped = append(stopped, node)
		}
	}

	if len(stopped) != 2 {
		t.Fatal("Expected exactly two stopped replicas")
	}

	before := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	t.Log("Quorum lost: one of three replicas remains")

	update := gatewayDistributedMutation(
		"quorum-recovery-update",
		map[string]string{"value": "200"},
	)

	code, failed := gatewayDistributedQuorumRequest(
		t,
		cluster,
		http.MethodPut,
		path,
		update,
	)

	if code != http.StatusGatewayTimeout ||
		failed.Code != "outcome_unknown" ||
		failed.Applied ||
		failed.Error == "" ||
		failed.ClientID != update["client_id"] ||
		failed.RequestID != update["request_id"] {
		t.Fatalf(
			"Unexpected mutation outcome: HTTP=%d response=%+v",
			code,
			failed,
		)
	}

	t.Logf(
		"Mutation without quorum: HTTP=%d code=%s",
		code,
		failed.Code,
	)

	for _, readPath := range []string{
		path,
		path + "?read_mode=linearizable",
	} {
		code, result := gatewayDistributedQuorumRequest(
			t,
			cluster,
			http.MethodGet,
			readPath,
			nil,
		)

		validError :=
			(code == http.StatusGatewayTimeout &&
				result.Code == "deadline_exceeded") ||
				(code == http.StatusServiceUnavailable &&
					result.Code == "leader_unavailable")

		if !validError ||
			result.Found ||
			result.Value != "" ||
			result.Error == "" {
			t.Fatalf(
				"Unexpected quorum-loss read: HTTP=%d response=%+v",
				code,
				result,
			)
		}

		t.Logf(
			"Linearizable read rejected: HTTP=%d code=%s",
			code,
			result.Code,
		)
	}

	gatewayDistributedRequireValue(
		t,
		cluster,
		path+"?read_mode=local",
		"100",
	)

	after := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, before, after,
	)

	t.Logf(
		"No commit or apply progress without quorum: commit=%d applied=%d",
		after.CommitIndex,
		after.AppliedIndex,
	)

	stopped[0].start(t, cluster.daemonBinary)

	gatewayDistributedWaitHTTP(
		t,
		stopped[0].httpAddr,
		"/status",
		http.StatusOK,
	)

	t.Log("Majority restored: two of three replicas")

	cluster.waitLeader(t, 0)

	gatewayDistributedWaitMajorityReady(
		t,
		cluster,
		preservedPath,
		"stable",
	)

	recovered := cluster.request(
		t,
		http.MethodPut,
		path,
		update,
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t,
		recovered,
		"quorum-recovery-update",
		true,
	)

	gatewayDistributedRequireValue(
		t, cluster, path, "200",
	)

	gatewayDistributedRequireValue(
		t, cluster, preservedPath, "stable",
	)

	t.Log("Original mutation identity resolved after recovery")

	recoveredLeader := cluster.waitLeader(t, 0)

	recoveredStatus := gatewayDistributedRequireStatus(
		t, cluster, recoveredLeader,
	)

	gatewayDistributedWaitReplication(
		t, cluster, recoveredStatus.CommitIndex,
	)

	replayBefore := gatewayDistributedRequireStatus(
		t, cluster, recoveredLeader,
	)

	for i := 0; i < 10; i++ {
		replay := cluster.request(
			t,
			http.MethodPut,
			path,
			update,
			http.StatusOK,
		)

		if replay != recovered {
			t.Fatalf(
				"Replay %d changed result: got=%+v want=%+v",
				i,
				replay,
				recovered,
			)
		}
	}

	replayAfter := gatewayDistributedRequireStatus(
		t, cluster, recoveredLeader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, replayBefore, replayAfter,
	)

	t.Logf(
		"10 recovery replays: commit index %d -> %d",
		replayBefore.CommitIndex,
		replayAfter.CommitIndex,
	)

	stopped[1].start(t, cluster.daemonBinary)

	gatewayDistributedWaitHTTP(
		t,
		stopped[1].httpAddr,
		"/status",
		http.StatusOK,
	)

	finalStatuses := gatewayDistributedWaitConvergence(
		t,
		cluster,
		replayAfter.CommitIndex,
	)

	for _, status := range finalStatuses {
		t.Logf(
			"Replica %d recovered: term=%d commit=%d applied=%d",
			status.ID,
			status.Term,
			status.CommitIndex,
			status.AppliedIndex,
		)
	}

	gatewayDistributedRequireValue(
		t, cluster, path, "200",
	)

	gatewayDistributedRequireValue(
		t, cluster, preservedPath, "stable",
	)

	if cluster.gateway.process == nil ||
		cluster.gateway.process.Process.Pid != gatewayPID {
		t.Fatal("Gateway process changed during recovery")
	}

	t.Logf("Gateway PID %d remained unchanged", gatewayPID)
	t.Log("PASS: Distributed quorum loss and recovery")
}

// Wait for all replicas to agree on a stable committed state.
func gatewayDistributedWaitConvergence(
	t *testing.T,
	cluster *gatewayDistributedCluster,
	minimumCommit uint64,
) []gatewayDistributedStatus {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)

	var last []gatewayDistributedStatus
	var previous gatewayDistributedStatus
	stableRounds := 0

	for time.Now().Before(deadline) {
		statuses := make(
			[]gatewayDistributedStatus,
			0,
			len(cluster.nodes),
		)

		valid := true
		leaders := 0
		var leader gatewayDistributedStatus

		for _, node := range cluster.nodes {
			status, err := cluster.status(node)
			if err != nil {
				valid = false
				break
			}

			statuses = append(statuses, status)

			if status.Role == raft.Leader {
				leaders++
				leader = status
			}
		}

		last = statuses

		if len(statuses) != len(cluster.nodes) ||
			leaders != 1 ||
			leader.LeaderID != leader.ID ||
			leader.CommitIndex < minimumCommit {
			valid = false
		}

		if valid {
			for _, status := range statuses {
				if status.Term != leader.Term ||
					status.LeaderID != leader.ID ||
					status.CommitIndex != leader.CommitIndex ||
					status.AppliedIndex != leader.CommitIndex {
					valid = false
					break
				}
			}
		}

		if valid {
			if leader == previous {
				stableRounds++
			} else {
				previous = leader
				stableRounds = 1
			}

			if stableRounds >= 5 {
				t.Logf(
					"Stable convergence: leader=%d term=%d commit=%d",
					leader.ID,
					leader.Term,
					leader.CommitIndex,
				)
				return statuses
			}
		} else {
			stableRounds = 0
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf(
		"Replicas did not converge within 30 seconds: minimumCommit=%d last=%+v",
		minimumCommit,
		last,
	)

	return nil
}

// A reported leader is not sufficient to establish quorum.
// A successful linearizable read confirms majority readiness.
func gatewayDistributedWaitMajorityReady(
	t *testing.T,
	cluster *gatewayDistributedCluster,
	path string,
	expected string,
) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	attempts := 0

	for time.Now().Before(deadline) {
		attempts++

		code, response := gatewayDistributedQuorumRequest(
			t,
			cluster,
			http.MethodGet,
			path+"?read_mode=linearizable",
			nil,
		)

		if code == http.StatusOK {
			if !response.Found ||
				response.Value != expected ||
				response.Error != "" {
				t.Fatalf(
					"Unexpected majority readiness result: %+v",
					response,
				)
			}

			t.Logf(
				"Majority ready after %d linearizable read attempts",
				attempts,
			)

			return
		}

		retryable :=
			(code == http.StatusGatewayTimeout &&
				response.Code == "deadline_exceeded") ||
				(code == http.StatusServiceUnavailable &&
					response.Code == "leader_unavailable")

		if !retryable {
			t.Fatalf(
				"Unexpected readiness failure: HTTP=%d response=%+v",
				code,
				response,
			)
		}

		t.Logf(
			"Majority not ready: attempt=%d HTTP=%d code=%s",
			attempts,
			code,
			response.Code,
		)

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf(
		"Majority readiness not established after %d attempts",
		attempts,
	)
}
