package main

import (
	"net/http"
	"testing"
)

func TestGatewayDistributedBackendUnavailable(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const putPath = "/kv/unavailable-put"
	const casPath = "/kv/unavailable-cas"
	const deletePath = "/kv/unavailable-delete"
	const preservedPath = "/kv/unavailable-preserved"

	gatewayPID := cluster.gateway.process.Process.Pid

	seeds := []struct {
		path  string
		id    string
		value string
	}{
		{putPath, "unavailable-put-seed", "100"},
		{casPath, "unavailable-cas-seed", "100"},
		{deletePath, "unavailable-delete-seed", "remove-me"},
		{preservedPath, "unavailable-preserved-seed", "stable"},
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

	t.Logf(
		"Healthy cluster: leader=%d term=%d commit=%d",
		leader.id,
		before.Term,
		before.CommitIndex,
	)

	for _, node := range cluster.nodes {
		node.stop(t)
	}

	for _, node := range cluster.nodes {
		if node.process != nil {
			t.Fatalf("Replica %d still running", node.id)
		}
	}

	if t.Failed() {
		t.FailNow()
	}

	t.Log("Complete backend outage: zero of three replicas running")

	for _, path := range []string{
		preservedPath,
		preservedPath + "?read_mode=linearizable",
		preservedPath + "?read_mode=local",
	} {
		code, response := gatewayDistributedQuorumRequest(
			t,
			cluster,
			http.MethodGet,
			path,
			nil,
		)

		valid :=
			(code == http.StatusServiceUnavailable &&
				response.Code == "leader_unavailable") ||
				(code == http.StatusGatewayTimeout &&
					response.Code == "deadline_exceeded")

		if !valid ||
			response.Found ||
			response.Value != "" ||
			response.Error == "" {
			t.Fatalf(
				"Unexpected outage read: path=%s HTTP=%d response=%+v",
				path,
				code,
				response,
			)
		}

		t.Logf(
			"Outage read rejected: path=%s HTTP=%d code=%s",
			path,
			code,
			response.Code,
		)
	}

	mutations := []struct {
		method string
		path   string
		id     string
		body   map[string]string
	}{
		{
			http.MethodPut,
			putPath,
			"unavailable-put-update",
			gatewayDistributedMutation(
				"unavailable-put-update",
				map[string]string{"value": "200"},
			),
		},
		{
			http.MethodPost,
			casPath + "/cas",
			"unavailable-cas-update",
			gatewayDistributedMutation(
				"unavailable-cas-update",
				map[string]string{
					"expected": "100",
					"value":    "200",
				},
			),
		},
		{
			http.MethodDelete,
			deletePath,
			"unavailable-delete-update",
			gatewayDistributedMutation(
				"unavailable-delete-update",
				nil,
			),
		},
	}

	for _, mutation := range mutations {
		code, response := gatewayDistributedQuorumRequest(
			t,
			cluster,
			mutation.method,
			mutation.path,
			mutation.body,
		)

		valid :=
			(code == http.StatusServiceUnavailable &&
				response.Code == "leader_unavailable") ||
				(code == http.StatusGatewayTimeout &&
					(response.Code == "outcome_unknown" ||
						response.Code == "deadline_exceeded"))

		if !valid ||
			response.Applied ||
			response.Error == "" ||
			response.ClientID != mutation.body["client_id"] ||
			response.RequestID != mutation.id {
			t.Fatalf(
				"Unexpected outage mutation: method=%s HTTP=%d response=%+v",
				mutation.method,
				code,
				response,
			)
		}

		t.Logf(
			"Outage mutation rejected: method=%s HTTP=%d code=%s identity=%s",
			mutation.method,
			code,
			response.Code,
			response.RequestID,
		)
	}

	gatewayDistributedWaitHTTP(
		t,
		cluster.gateway.httpAddr,
		"/nonexistent",
		http.StatusNotFound,
	)

	if cluster.gateway.process == nil ||
		cluster.gateway.process.Process.Pid != gatewayPID {
		t.Fatal("Gateway process changed during outage")
	}

	t.Logf("Gateway remained responsive: PID=%d", gatewayPID)

	for _, node := range cluster.nodes {
		node.start(t, cluster.daemonBinary)
	}

	for _, node := range cluster.nodes {
		gatewayDistributedWaitHTTP(
			t,
			node.httpAddr,
			"/status",
			http.StatusOK,
		)
	}

	cluster.waitLeader(t, 0)

	gatewayDistributedWaitMajorityReady(
		t,
		cluster,
		preservedPath,
		"stable",
	)

	gatewayDistributedWaitConvergence(
		t,
		cluster,
		before.CommitIndex,
	)

	for _, seed := range seeds {
		gatewayDistributedRequireValue(
			t,
			cluster,
			seed.path,
			seed.value,
		)
	}

	t.Log("All four previously committed keys survived backend restart")

	recoveredLeader := cluster.waitLeader(t, 0)

	recoveryBefore := gatewayDistributedRequireStatus(
		t, cluster, recoveredLeader,
	)

	results := make(
		[]gatewayDistributedResponse,
		len(mutations),
	)

	for i, mutation := range mutations {
		results[i] = cluster.request(
			t,
			mutation.method,
			mutation.path,
			mutation.body,
			http.StatusOK,
		)

		gatewayDistributedRequireMutation(
			t,
			results[i],
			mutation.id,
			true,
		)
	}

	gatewayDistributedRequireValue(
		t, cluster, putPath, "200",
	)

	gatewayDistributedRequireValue(
		t, cluster, casPath, "200",
	)

	deleted := cluster.request(
		t,
		http.MethodGet,
		deletePath,
		nil,
		http.StatusNotFound,
	)

	if deleted.Found ||
		deleted.Value != "" ||
		deleted.Code != "key_not_found" {
		t.Fatalf("DELETE recovery failed: %+v", deleted)
	}

	gatewayDistributedRequireValue(
		t, cluster, preservedPath, "stable",
	)

	recoveryAfter := gatewayDistributedRequireStatus(
		t, cluster, recoveredLeader,
	)

	gatewayDistributedRequireCommitDelta(
		t,
		recoveryBefore,
		recoveryAfter,
		uint64(len(mutations)),
	)

	gatewayDistributedWaitConvergence(
		t,
		cluster,
		recoveryAfter.CommitIndex,
	)

	for i, mutation := range mutations {
		replay := cluster.request(
			t,
			mutation.method,
			mutation.path,
			mutation.body,
			http.StatusOK,
		)

		if replay != results[i] {
			t.Fatalf(
				"Recovered mutation replay changed: got=%+v want=%+v",
				replay,
				results[i],
			)
		}
	}

	final := gatewayDistributedRequireStatus(
		t, cluster, recoveredLeader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, recoveryAfter, final,
	)

	gatewayDistributedWaitConvergence(
		t,
		cluster,
		final.CommitIndex,
	)

	gatewayDistributedVerifyReplicaData(
		t,
		cluster,
		[]gatewayReplicaExpectedValue{
			{key: "unavailable-put", value: "200", found: true},
			{key: "unavailable-cas", value: "200", found: true},
			{key: "unavailable-delete", found: false},
			{key: "unavailable-preserved", value: "stable", found: true},
		},
	)

	if cluster.gateway.process == nil ||
		cluster.gateway.process.Process.Pid != gatewayPID {
		t.Fatal("Gateway process changed during recovery")
	}

	t.Log("Recovered mutations: PUT=1 CAS=1 DELETE=1")
	t.Log("Verified replays=3 additional_commits=0")
	t.Logf("Gateway PID %d remained unchanged", gatewayPID)
	t.Log("PASS: Complete backend unavailability and recovery")
}
