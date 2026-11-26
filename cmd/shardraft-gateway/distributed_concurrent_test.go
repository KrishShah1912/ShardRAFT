package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
)

const gatewayDistributedConcurrentWorkers = 32

type gatewayDistributedConcurrentRequest struct {
	method string
	path   string
	body   any
}

type gatewayDistributedConcurrentResult struct {
	status   int
	response gatewayDistributedResponse
	err      error
}

func gatewayDistributedExecuteConcurrentRequest(
	cluster *gatewayDistributedCluster,
	input gatewayDistributedConcurrentRequest,
) gatewayDistributedConcurrentResult {
	var result gatewayDistributedConcurrentResult
	var payload []byte

	if input.body != nil {
		payload, result.err = json.Marshal(input.body)
		if result.err != nil {
			return result
		}
	}

	request, err := http.NewRequest(
		input.method,
		cluster.gatewayURL+input.path,
		bytes.NewReader(payload),
	)

	if err != nil {
		result.err = err
		return result
	}

	if input.body != nil {
		request.Header.Set(
			"Content-Type",
			"application/json",
		)
	}

	response, err := cluster.httpClient.Do(request)

	if err != nil {
		result.err = err
		return result
	}

	defer response.Body.Close()

	result.status = response.StatusCode

	data, err := io.ReadAll(response.Body)

	if err != nil {
		result.err = err
		return result
	}

	if err := json.Unmarshal(
		data,
		&result.response,
	); err != nil {
		result.err = fmt.Errorf(
			"invalid response: %s: %w",
			data,
			err,
		)
	}

	return result
}

func gatewayDistributedRunConcurrent(
	t *testing.T,
	cluster *gatewayDistributedCluster,
	requests []gatewayDistributedConcurrentRequest,
) []gatewayDistributedConcurrentResult {
	t.Helper()

	results := make(
		[]gatewayDistributedConcurrentResult,
		len(requests),
	)

	start := make(chan struct{})

	var ready sync.WaitGroup
	var finished sync.WaitGroup

	ready.Add(len(requests))
	finished.Add(len(requests))

	for i, request := range requests {
		go func(
			index int,
			input gatewayDistributedConcurrentRequest,
		) {
			defer finished.Done()

			ready.Done()
			<-start

			results[index] =
				gatewayDistributedExecuteConcurrentRequest(
					cluster,
					input,
				)
		}(i, request)
	}

	ready.Wait()
	close(start)
	finished.Wait()

	for i, result := range results {
		if result.err != nil {
			t.Errorf(
				"Worker %d failed: %v",
				i,
				result.err,
			)
		}
	}

	if t.Failed() {
		t.FailNow()
	}

	return results
}

func gatewayDistributedRequireCommitDelta(
	t *testing.T,
	before gatewayDistributedStatus,
	after gatewayDistributedStatus,
	expected uint64,
) {
	t.Helper()

	if after.Term != before.Term ||
		after.ID != before.ID ||
		after.CommitIndex != before.CommitIndex+expected ||
		after.AppliedIndex != after.CommitIndex {
		t.Fatalf(
			"Unexpected Raft progress: before=%+v after=%+v expected_delta=%d",
			before,
			after,
			expected,
		)
	}

	t.Logf(
		"Commit index: %d -> %d; delta=%d",
		before.CommitIndex,
		after.CommitIndex,
		expected,
	)
}

func TestGatewayDistributedConcurrentPut(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const workers = gatewayDistributedConcurrentWorkers

	seed := cluster.request(
		t,
		http.MethodPut,
		"/kv/concurrent-put-seed",
		gatewayDistributedMutation(
			"concurrent-put-seed",
			map[string]string{"value": "ready"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t,
		seed,
		"concurrent-put-seed",
		true,
	)

	gatewayDistributedRequireValue(
		t,
		cluster,
		"/kv/concurrent-put-seed",
		"ready",
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

	for i := range requests {
		requests[i] = gatewayDistributedConcurrentRequest{
			method: http.MethodPut,
			path: fmt.Sprintf(
				"/kv/concurrent-put-key-%03d",
				i,
			),
			body: gatewayDistributedMutation(
				fmt.Sprintf("concurrent-put-%03d", i),
				map[string]string{
					"value": fmt.Sprintf("value-%03d", i),
				},
			),
		}
	}

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
			fmt.Sprintf("concurrent-put-%03d", i),
			true,
		)

		expected := fmt.Sprintf("value-%03d", i)

		if result.response.Value != expected {
			t.Fatalf(
				"Worker %d: value=%q want=%q",
				i,
				result.response.Value,
				expected,
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
		uint64(workers),
	)

	for i, request := range requests {
		gatewayDistributedRequireValue(
			t,
			cluster,
			request.path+"?read_mode=linearizable",
			fmt.Sprintf("value-%03d", i),
		)
	}

	statuses := gatewayDistributedWaitConvergence(
		t,
		cluster,
		after.CommitIndex,
	)

	for _, status := range statuses {
		if status.Term != before.Term ||
			status.LeaderID != before.ID ||
			status.CommitIndex != before.CommitIndex+uint64(workers) ||
			status.AppliedIndex != status.CommitIndex {
			t.Fatalf(
				"Replica %d has unexpected state: before=%+v after=%+v",
				status.ID,
				before,
				status,
			)
		}

		t.Logf(
			"Replica %d synchronized: term=%d commit=%d applied=%d",
			status.ID,
			status.Term,
			status.CommitIndex,
			status.AppliedIndex,
		)
	}

	t.Logf(
		"Concurrent PUT: workers=%d successful=%d verified_keys=%d replicas=%d",
		workers,
		len(results),
		workers,
		len(statuses),
	)
}
