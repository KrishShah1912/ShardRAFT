package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

func dedupLocalRouter(node *shard.Shard) *requestRouter {
	return newRequestRouter(
		statusHandler(node),
		func(context.Context) (leaderTarget, error) {
			return leaderTarget{}, errLeaderUnavailable
		},
		node.Status,
	)
}

func dedupRemoteRouter(server *httptest.Server) *requestRouter {
	return newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			return leaderTarget{
				ID:      1,
				Address: strings.TrimPrefix(server.URL, "http://"),
			}, nil
		},
		routerFollowerStatus,
	)
}

func dedupAppliedIndex(t *testing.T, node *shard.Shard) uint64 {
	t.Helper()

	index, err := node.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}

	return index
}

func dedupRequireResponse(
	t *testing.T,
	rec *httptest.ResponseRecorder,
	status int,
	body string,
) {
	t.Helper()

	if rec.Code != status {
		t.Fatalf(
			"status=%d, want=%d; body=%s",
			rec.Code, status, rec.Body.String(),
		)
	}

	if body != "" && rec.Body.String() != body {
		t.Fatalf(
			"body=%q, want=%q",
			rec.Body.String(), body,
		)
	}
}

func dedupRequireValue(
	t *testing.T,
	node *shard.Shard,
	key, expected string,
	exists bool,
) {
	t.Helper()

	value, found, err := node.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if found != exists || value != expected {
		t.Fatalf(
			"key=%q value=%q found=%v; want value=%q found=%v",
			key, value, found, expected, exists,
		)
	}
}

func dedupRequireUnchangedIndex(
	t *testing.T,
	node *shard.Shard,
	before uint64,
) {
	t.Helper()

	after := dedupAppliedIndex(t, node)

	if after != before {
		t.Fatalf(
			"duplicate advanced applied index: %d -> %d",
			before, after,
		)
	}
}

func TestRequestDedupDuplicatePUT(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	router := dedupLocalRouter(node)

	payload := `{"value":"Go","client_id":"dedup-client","request_id":"put-1"}`

	first := sendKVRequest(
		t, router, http.MethodPut, "/kv/language", payload,
	)

	dedupRequireResponse(
		t, first, http.StatusOK,
		"{\"applied\":true,\"value\":\"Go\"}\n",
	)

	before := dedupAppliedIndex(t, node)

	second := sendKVRequest(
		t, router, http.MethodPut, "/kv/language", payload,
	)

	dedupRequireResponse(
		t, second, http.StatusOK, first.Body.String(),
	)

	dedupRequireUnchangedIndex(t, node, before)
	dedupRequireValue(t, node, "language", "Go", true)
}

func TestRequestDedupDuplicateDELETE(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	router := dedupLocalRouter(node)

	put := `{"value":"Go","client_id":"dedup-client","request_id":"put-1"}`

	rec := sendKVRequest(
		t, router, http.MethodPut, "/kv/language", put,
	)

	dedupRequireResponse(t, rec, http.StatusOK, "")

	payload := `{"client_id":"dedup-client","request_id":"delete-1"}`

	first := sendKVRequest(
		t, router, http.MethodDelete, "/kv/language", payload,
	)

	dedupRequireResponse(
		t, first, http.StatusOK,
		"{\"applied\":true}\n",
	)

	before := dedupAppliedIndex(t, node)

	second := sendKVRequest(
		t, router, http.MethodDelete, "/kv/language", payload,
	)

	dedupRequireResponse(
		t, second, http.StatusOK, first.Body.String(),
	)

	dedupRequireUnchangedIndex(t, node, before)
	dedupRequireValue(t, node, "language", "", false)
}

func TestRequestDedupDuplicateCAS(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	router := dedupLocalRouter(node)

	put := `{"value":"Go","client_id":"dedup-client","request_id":"put-1"}`

	rec := sendKVRequest(
		t, router, http.MethodPut, "/kv/language", put,
	)

	dedupRequireResponse(t, rec, http.StatusOK, "")

	payload := `{"expected":"Go","value":"Python","client_id":"dedup-client","request_id":"cas-1"}`

	first := sendKVRequest(
		t, router, http.MethodPost, "/kv/language/cas", payload,
	)

	dedupRequireResponse(
		t, first, http.StatusOK,
		"{\"applied\":true,\"value\":\"Python\"}\n",
	)

	before := dedupAppliedIndex(t, node)

	second := sendKVRequest(
		t, router, http.MethodPost, "/kv/language/cas", payload,
	)

	dedupRequireResponse(
		t, second, http.StatusOK, first.Body.String(),
	)

	dedupRequireUnchangedIndex(t, node, before)
	dedupRequireValue(t, node, "language", "Python", true)
}

func TestRequestDedupConflictingPayload(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	router := dedupLocalRouter(node)

	first := `{"value":"Go","client_id":"dedup-client","request_id":"same-id"}`
	conflict := `{"value":"Python","client_id":"dedup-client","request_id":"same-id"}`

	rec := sendKVRequest(
		t, router, http.MethodPut, "/kv/language", first,
	)

	dedupRequireResponse(t, rec, http.StatusOK, "")

	before := dedupAppliedIndex(t, node)

	rec = sendKVRequest(
		t, router, http.MethodPut, "/kv/language", conflict,
	)

	dedupRequireResponse(t, rec, http.StatusConflict, "")

	dedupRequireUnchangedIndex(t, node, before)
	dedupRequireValue(t, node, "language", "Go", true)
}

func TestRequestDedupLostResponseRetry(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	leader := dedupLocalRouter(node)

	put := `{"value":"Go","client_id":"dedup-client","request_id":"put-1"}`

	rec := sendKVRequest(
		t, leader, http.MethodPut, "/kv/language", put,
	)

	dedupRequireResponse(t, rec, http.StatusOK, "")

	before := dedupAppliedIndex(t, node)

	var calls atomic.Int32

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			result := httptest.NewRecorder()
			leader.ServeHTTP(result, r)

			if result.Code != http.StatusOK {
				t.Errorf(
					"leader returned %d: %s",
					result.Code, result.Body.String(),
				)
			}

			if calls.Add(1) == 1 {
				// The mutation has committed, but the
				// successful response is not delivered.
				writeRoutingError(
					w,
					http.StatusGatewayTimeout,
					routingCodeUnknown,
					"simulated lost response",
					raft.Status{},
				)
				return
			}

			w.Header().Set(
				"Content-Type",
				result.Header().Get("Content-Type"),
			)

			w.WriteHeader(result.Code)
			_, _ = w.Write(result.Body.Bytes())
		}),
	)
	defer server.Close()

	follower := dedupRemoteRouter(server)

	payload := `{"expected":"Go","value":"Python","client_id":"dedup-client","request_id":"cas-1"}`

	rec = sendKVRequest(
		t, follower, http.MethodPost, "/kv/language/cas", payload,
	)

	dedupRequireResponse(
		t, rec, http.StatusOK,
		"{\"applied\":true,\"value\":\"Python\"}\n",
	)

	if calls.Load() != 2 {
		t.Fatalf("expected two attempts, got %d", calls.Load())
	}

	after := dedupAppliedIndex(t, node)

	if after != before+1 {
		t.Fatalf(
			"expected exactly one applied entry: %d -> %d",
			before, after,
		)
	}

	dedupRequireValue(t, node, "language", "Python", true)
}

func TestRequestDedupDoesNotAdvanceLog(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	router := dedupLocalRouter(node)

	payload := `{"value":"Go","client_id":"dedup-client","request_id":"put-1"}`

	first := sendKVRequest(
		t, router, http.MethodPut, "/kv/language", payload,
	)

	dedupRequireResponse(t, first, http.StatusOK, "")

	before := dedupAppliedIndex(t, node)

	statusBefore, err := node.Status()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		rec := sendKVRequest(
			t, router, http.MethodPut, "/kv/language", payload,
		)

		dedupRequireResponse(
			t, rec, http.StatusOK, first.Body.String(),
		)
	}

	dedupRequireUnchangedIndex(t, node, before)

	statusAfter, err := node.Status()
	if err != nil {
		t.Fatal(err)
	}

	if statusAfter.Commit != statusBefore.Commit {
		t.Fatalf(
			"commit index changed: %d -> %d",
			statusBefore.Commit, statusAfter.Commit,
		)
	}
}

func TestRequestDedupAcrossEntryRouters(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	leader := dedupLocalRouter(node)

	server := httptest.NewServer(leader)
	defer server.Close()

	entryA := dedupRemoteRouter(server)
	entryB := dedupRemoteRouter(server)

	payload := `{"value":"Go","client_id":"dedup-client","request_id":"put-1"}`

	first := sendKVRequest(
		t, entryA, http.MethodPut, "/kv/language", payload,
	)

	dedupRequireResponse(t, first, http.StatusOK, "")

	before := dedupAppliedIndex(t, node)

	for _, router := range []*requestRouter{entryB, leader, entryA} {
		rec := sendKVRequest(
			t, router, http.MethodPut, "/kv/language", payload,
		)

		dedupRequireResponse(
			t, rec, http.StatusOK, first.Body.String(),
		)
	}

	dedupRequireUnchangedIndex(t, node, before)
	dedupRequireValue(t, node, "language", "Go", true)
}

func TestRequestDedupFailedCASResult(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	router := dedupLocalRouter(node)

	put := `{"value":"Go","client_id":"dedup-client","request_id":"put-1"}`

	rec := sendKVRequest(
		t, router, http.MethodPut, "/kv/language", put,
	)

	dedupRequireResponse(t, rec, http.StatusOK, "")

	cas := `{"expected":"Python","value":"Java","client_id":"dedup-client","request_id":"cas-1"}`

	first := sendKVRequest(
		t, router, http.MethodPost, "/kv/language/cas", cas,
	)

	dedupRequireResponse(
		t, first, http.StatusOK,
		"{\"applied\":false}\n",
	)

	// Make the original CAS condition true afterward.
	update := `{"value":"Python","client_id":"dedup-client","request_id":"put-2"}`

	rec = sendKVRequest(
		t, router, http.MethodPut, "/kv/language", update,
	)

	dedupRequireResponse(t, rec, http.StatusOK, "")

	before := dedupAppliedIndex(t, node)

	rec = sendKVRequest(
		t, router, http.MethodPost, "/kv/language/cas", cas,
	)

	dedupRequireResponse(
		t, rec, http.StatusOK, first.Body.String(),
	)

	dedupRequireUnchangedIndex(t, node, before)
	dedupRequireValue(t, node, "language", "Python", true)
}

func TestRequestDedupAfterRestart(t *testing.T) {
	cfg := shard.Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         filepath.Join(t.TempDir(), "raft.wal"),
		Send: func([]raft.Message) error {
			return nil
		},
	}

	openLeader := func() *shard.Shard {
		t.Helper()

		node, err := shard.Open(cfg)
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

	node := openLeader()
	router := dedupLocalRouter(node)

	put := `{"value":"Go","client_id":"dedup-client","request_id":"put-1"}`

	rec := sendKVRequest(
		t, router, http.MethodPut, "/kv/language", put,
	)

	dedupRequireResponse(t, rec, http.StatusOK, "")

	cas := `{"expected":"Go","value":"Python","client_id":"dedup-client","request_id":"cas-1"}`

	first := sendKVRequest(
		t, router, http.MethodPost, "/kv/language/cas", cas,
	)

	dedupRequireResponse(
		t, first, http.StatusOK,
		"{\"applied\":true,\"value\":\"Python\"}\n",
	)

	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := openLeader()
	router = dedupLocalRouter(restarted)

	before := dedupAppliedIndex(t, restarted)

	rec = sendKVRequest(
		t, router, http.MethodPost, "/kv/language/cas", cas,
	)

	dedupRequireResponse(
		t, rec, http.StatusOK, first.Body.String(),
	)

	dedupRequireUnchangedIndex(t, restarted, before)

	conflict := `{"expected":"Go","value":"Java","client_id":"dedup-client","request_id":"cas-1"}`

	rec = sendKVRequest(
		t, router, http.MethodPost, "/kv/language/cas", conflict,
	)

	dedupRequireResponse(t, rec, http.StatusConflict, "")

	dedupRequireUnchangedIndex(t, restarted, before)
	dedupRequireValue(t, restarted, "language", "Python", true)
}
