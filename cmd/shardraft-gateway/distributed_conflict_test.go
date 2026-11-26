package main

import (
	"net/http"
	"testing"
)

func gatewayDistributedRequireConflict(
	t *testing.T,
	result gatewayDistributedResponse,
	requestID string,
) {
	t.Helper()

	if result.Code != "request_conflict" ||
		result.Error == "" ||
		result.ClientID != "gateway-integration-client" ||
		result.RequestID != requestID ||
		result.Applied {
		t.Fatalf(
			"Unexpected conflict response: %+v",
			result,
		)
	}
}

func TestGatewayDistributedRequestConflicts(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const path = "/kv/conflict-key"
	const requestID = "conflict-original"

	leader := cluster.waitLeader(t, 0)

	originalBody := gatewayDistributedMutation(
		requestID,
		map[string]string{"value": "100"},
	)

	original := cluster.request(
		t,
		http.MethodPut,
		path,
		originalBody,
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, original, requestID, true,
	)

	before := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedWaitReplication(
		t, cluster, before.CommitIndex,
	)

	tests := []struct {
		name   string
		method string
		path   string
		fields map[string]string
	}{
		{
			name:   "DifferentValue",
			method: http.MethodPut,
			path:   path,
			fields: map[string]string{
				"value": "999",
			},
		},
		{
			name:   "DifferentKey",
			method: http.MethodPut,
			path:   "/kv/conflict-other-key",
			fields: map[string]string{
				"value": "100",
			},
		},
		{
			name:   "DifferentDeleteOperation",
			method: http.MethodDelete,
			path:   path,
		},
		{
			name:   "DifferentCASOperation",
			method: http.MethodPost,
			path:   path + "/cas",
			fields: map[string]string{
				"expected": "100",
				"value":    "999",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := cluster.request(
				t,
				tc.method,
				tc.path,
				gatewayDistributedMutation(
					requestID,
					tc.fields,
				),
				http.StatusConflict,
			)

			gatewayDistributedRequireConflict(
				t, result, requestID,
			)

			after := gatewayDistributedRequireStatus(
				t, cluster, leader,
			)

			gatewayDistributedRequireUnchangedIndexes(
				t, before, after,
			)

			gatewayDistributedRequireValue(
				t, cluster, path, "100",
			)

			t.Logf(
				"Conflict rejected: %s; commit index=%d",
				tc.name,
				after.CommitIndex,
			)
		})
	}

	missing := cluster.request(
		t,
		http.MethodGet,
		"/kv/conflict-other-key",
		nil,
		http.StatusNotFound,
	)

	if missing.Code != "key_not_found" {
		t.Fatalf(
			"Unexpected missing-key response: %+v",
			missing,
		)
	}

	replay := cluster.request(
		t,
		http.MethodPut,
		path,
		originalBody,
		http.StatusOK,
	)

	if replay != original {
		t.Fatalf(
			"Original replay changed: original=%+v replay=%+v",
			original,
			replay,
		)
	}

	after := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, before, after,
	)

	t.Log("All conflicting requests rejected without additional commits")
}

func TestGatewayDistributedClientIdentityIsolation(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const path = "/kv/identity-isolation"
	const requestID = "shared-request-id"

	leader := cluster.waitLeader(t, 0)

	first := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			requestID,
			map[string]string{"value": "100"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, first, requestID, true,
	)

	before := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	secondBody := map[string]string{
		"client_id":  "second-integration-client",
		"request_id": requestID,
		"value":      "200",
	}

	second := cluster.request(
		t,
		http.MethodPut,
		path,
		secondBody,
		http.StatusOK,
	)

	if !second.Applied ||
		second.ClientID != "second-integration-client" ||
		second.RequestID != requestID ||
		second.Error != "" {
		t.Fatalf(
			"Independent client mutation failed: %+v",
			second,
		)
	}

	gatewayDistributedRequireValue(
		t, cluster, path, "200",
	)

	after := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	if after.Term != before.Term ||
		after.CommitIndex != before.CommitIndex+1 {
		t.Fatalf(
			"Expected one additional commit: before=%+v after=%+v",
			before,
			after,
		)
	}

	before = after

	firstReplay := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			requestID,
			map[string]string{"value": "100"},
		),
		http.StatusOK,
	)

	secondReplay := cluster.request(
		t,
		http.MethodPut,
		path,
		secondBody,
		http.StatusOK,
	)

	if firstReplay != first ||
		secondReplay != second {
		t.Fatal("Independent client replay results changed")
	}

	gatewayDistributedRequireValue(
		t, cluster, path, "200",
	)

	after = gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, before, after,
	)

	t.Log("Same request ID accepted for independent client IDs")
	t.Log("Both client identities replay independently")
}

func TestGatewayDistributedConflictAfterFailover(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const path = "/kv/failover-conflict"
	const requestID = "failover-conflict-original"

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

	before := gatewayDistributedRequireStatus(
		t, cluster, leader,
	)

	gatewayDistributedWaitReplication(
		t, cluster, before.CommitIndex,
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

	conflict := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			requestID,
			map[string]string{"value": "999"},
		),
		http.StatusConflict,
	)

	gatewayDistributedRequireConflict(
		t, conflict, requestID,
	)

	replay := cluster.request(
		t,
		http.MethodPut,
		path,
		body,
		http.StatusOK,
	)

	if replay != original {
		t.Fatal("Original response changed after failover")
	}

	gatewayDistributedRequireValue(
		t, cluster, path, "100",
	)

	after := gatewayDistributedRequireStatus(
		t, cluster, replacement,
	)

	gatewayDistributedRequireUnchangedIndexes(
		t, before, after,
	)

	t.Logf(
		"Replacement leader %d preserved conflict detection",
		replacement.id,
	)

	t.Log("Cross-leader conflict rejected without additional commits")
}
