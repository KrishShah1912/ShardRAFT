package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

const (
	gateway1 = "http://127.0.0.1:18091"
	gateway2 = "http://127.0.0.1:18092"
)

var httpClient = &http.Client{
	Timeout: 15 * time.Second,
}

type kvResponse struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Found     bool   `json:"found"`
	Applied   bool   `json:"applied"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

func requireDocker(t *testing.T) {
	t.Helper()

	if os.Getenv("SHARDRAFT_DOCKER_TEST") != "1" {
		t.Skip("set SHARDRAFT_DOCKER_TEST=1 to run Docker integration tests")
	}
}

func shardForKey(key string) uint32 {
	slot := crc32.ChecksumIEEE([]byte(key)) % 65536
	return slot/16384 + 1
}

func keyForShard(
	t *testing.T,
	prefix string,
	shard uint32,
) string {
	t.Helper()

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)

		if shardForKey(key) == shard {
			return key
		}
	}

	t.Fatalf("could not find key for shard %d", shard)
	return ""
}

func request(
	t *testing.T,
	method string,
	url string,
	body any,
	expectedStatus int,
) kvResponse {
	t.Helper()

	var reader io.Reader

	if body != nil {
		data, err := json.Marshal(body)

		if err != nil {
			t.Fatal(err)
		}

		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, reader)

	if err != nil {
		t.Fatal(err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpClient.Do(req)

	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)

	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != expectedStatus {
		t.Fatalf(
			"%s %s: status=%d want=%d body=%s",
			method,
			url,
			resp.StatusCode,
			expectedStatus,
			data,
		)
	}

	var result kvResponse

	if len(data) != 0 {
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("invalid response: %v body=%s", err, data)
		}
	}

	return result
}

func put(
	t *testing.T,
	gateway string,
	key string,
	value string,
	clientID string,
	requestID string,
) kvResponse {
	t.Helper()

	return request(
		t,
		http.MethodPut,
		gateway+"/kv/"+key,
		map[string]string{
			"value":      value,
			"client_id":  clientID,
			"request_id": requestID,
		},
		http.StatusOK,
	)
}

func requireValue(
	t *testing.T,
	gateway string,
	key string,
	expected string,
) {
	t.Helper()

	result := request(
		t,
		http.MethodGet,
		gateway+"/kv/"+key,
		nil,
		http.StatusOK,
	)

	if !result.Found || result.Value != expected {
		t.Fatalf(
			"key=%s found=%t value=%q want=%q",
			key,
			result.Found,
			result.Value,
			expected,
		)
	}
}

func TestDockerMultiShardIntegration(t *testing.T) {
	requireDocker(t)

	runID := fmt.Sprintf(
		"docker-integration-%d",
		time.Now().UnixNano(),
	)

	keys := make(map[uint32]string)

	for shard := uint32(1); shard <= 4; shard++ {
		key := keyForShard(
			t,
			fmt.Sprintf("%s-s%d", runID, shard),
			shard,
		)

		keys[shard] = key

		value := fmt.Sprintf("value-shard-%d", shard)

		result := put(
			t,
			gateway1,
			key,
			value,
			runID,
			fmt.Sprintf("put-%d", shard),
		)

		if !result.Applied {
			t.Fatalf("PUT failed for shard %d", shard)
		}

		requireValue(t, gateway2, key, value)

		t.Logf(
			"shard=%d slot=%d key=%s cross-gateway PUT/GET passed",
			shard,
			crc32.ChecksumIEEE([]byte(key))%65536,
			key,
		)
	}

	t.Run("DuplicateRequest", func(t *testing.T) {
		key := keys[1]

		result := put(
			t,
			gateway2,
			key,
			"updated-shard-1",
			runID,
			"update-1",
		)

		if !result.Applied {
			t.Fatal("update failed")
		}

		duplicate := put(
			t,
			gateway1,
			key,
			"value-shard-1",
			runID,
			"put-1",
		)

		if !duplicate.Applied {
			t.Fatal("duplicate did not return original success")
		}

		requireValue(
			t,
			gateway2,
			key,
			"updated-shard-1",
		)

		t.Log("duplicate request did not overwrite newer value")
	})

	t.Run("CompareAndSwap", func(t *testing.T) {
		key := keys[2]

		result := request(
			t,
			http.MethodPost,
			gateway2+"/kv/"+key+"/cas",
			map[string]string{
				"expected":   "value-shard-2",
				"value":      "cas-updated",
				"client_id":  runID,
				"request_id": "cas-1",
			},
			http.StatusOK,
		)

		if !result.Applied {
			t.Fatal("CAS should have succeeded")
		}

		requireValue(t, gateway1, key, "cas-updated")

		mismatch := request(
			t,
			http.MethodPost,
			gateway1+"/kv/"+key+"/cas",
			map[string]string{
				"expected":   "incorrect-value",
				"value":      "must-not-apply",
				"client_id":  runID,
				"request_id": "cas-2",
			},
			http.StatusOK,
		)

		if mismatch.Applied {
			t.Fatal("CAS with incorrect expected value applied")
		}

		requireValue(t, gateway2, key, "cas-updated")

		t.Log("CAS success and mismatch verified")
	})

	t.Run("Delete", func(t *testing.T) {
		key := keys[3]

		result := request(
			t,
			http.MethodDelete,
			gateway2+"/kv/"+key,
			map[string]string{
				"client_id":  runID,
				"request_id": "delete-1",
			},
			http.StatusOK,
		)

		if !result.Applied {
			t.Fatal("DELETE failed")
		}

		request(
			t,
			http.MethodGet,
			gateway1+"/kv/"+key,
			nil,
			http.StatusNotFound,
		)

		t.Log("DELETE and missing-key response verified")
	})

	t.Run("ShardIsolation", func(t *testing.T) {
		requireValue(
			t,
			gateway1,
			keys[1],
			"updated-shard-1",
		)

		requireValue(
			t,
			gateway2,
			keys[2],
			"cas-updated",
		)

		requireValue(
			t,
			gateway1,
			keys[4],
			"value-shard-4",
		)

		t.Log("operations preserved values in other shards")
	})
}
