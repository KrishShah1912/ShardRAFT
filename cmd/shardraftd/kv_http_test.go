package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

func newLeaderForHTTPTest(t *testing.T) *shard.Shard {
	t.Helper()

	node, err := shard.Open(shard.Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         filepath.Join(t.TempDir(), "raft.wal"),
		Send: func([]raft.Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})

	for i := 0; i < 30; i++ {
		if err := node.Tick(); err != nil {
			t.Fatal(err)
		}
		status, err := node.Status()
		if err != nil {
			t.Fatal(err)
		}
		if status.Role == raft.Leader {
			return node
		}
	}

	t.Fatal("node did not become leader")
	return nil
}

func sendKVRequest(
	t *testing.T,
	handler http.Handler,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(
		method,
		path,
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestHTTPKVPutAndGet(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	body := `{
		"value":"Go",
		"client_id":"client-1",
		"request_id":"request-1"
	}`

	rec := sendKVRequest(
		t, handler, http.MethodPut, "/kv/language", body,
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s",
			rec.Code, rec.Body.String())
	}

	var result struct {
		Applied bool   `json:"applied"`
		Value   string `json:"value"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.Value != "Go" {
		t.Fatalf("unexpected PUT result: %+v", result)
	}

	rec = sendKVRequest(
		t, handler, http.MethodGet, "/kv/language", "",
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status=%d body=%s",
			rec.Code, rec.Body.String())
	}

	var value struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if value.Key != "language" || value.Value != "Go" {
		t.Fatalf("unexpected GET result: %+v", value)
	}
}

func TestHTTPKVDuplicateRequest(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	body := `{
		"value":"Go",
		"client_id":"client-1",
		"request_id":"request-1"
	}`

	for i := 0; i < 2; i++ {
		rec := sendKVRequest(
			t, handler, http.MethodPut, "/kv/language", body,
		)
		if rec.Code != http.StatusOK {
			t.Fatalf("attempt %d: status=%d body=%s",
				i+1, rec.Code, rec.Body.String())
		}
	}
}

func TestHTTPKVConflictingRequestID(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	first := `{
		"value":"Go",
		"client_id":"client-1",
		"request_id":"request-1"
	}`

	conflict := `{
		"value":"Python",
		"client_id":"client-1",
		"request_id":"request-1"
	}`

	rec := sendKVRequest(
		t, handler, http.MethodPut, "/kv/language", first,
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("first PUT status=%d", rec.Code)
	}

	rec = sendKVRequest(
		t, handler, http.MethodPut, "/kv/language", conflict,
	)
	if rec.Code != http.StatusConflict {
		t.Fatalf("conflicting PUT status=%d, want 409",
			rec.Code)
	}
}

func TestHTTPKVMissingKey(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	rec := sendKVRequest(
		t, handler, http.MethodGet, "/kv/missing", "",
	)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET missing status=%d, want 404",
			rec.Code)
	}
}

func TestHTTPKVInvalidRequests(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	cases := []struct {
		name string
		body string
	}{
		{"invalid JSON", `{invalid`},
		{"missing request ID", `{"value":"Go","client_id":"c1"}`},
		{"missing client ID", `{"value":"Go","request_id":"r1"}`},
		{"unknown field", `{"value":"Go","client_id":"c1","request_id":"r1","unknown":1}`},
		{"multiple objects", `{"value":"Go","client_id":"c1","request_id":"r1"} {}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := sendKVRequest(
				t, handler, http.MethodPut, "/kv/test", tc.body,
			)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400; body=%s",
					rec.Code, rec.Body.String())
			}
		})
	}
}
