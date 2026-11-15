package main

import (
	"net/http"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

func TestHTTPKVRecoveryAfterRestart(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "raft.wal")

	cfg := shard.Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         walPath,
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

		for i := 0; i < 30; i++ {
			if err := node.Tick(); err != nil {
				node.Close()
				t.Fatal(err)
			}

			status, err := node.Status()
			if err != nil {
				node.Close()
				t.Fatal(err)
			}

			if status.Role == raft.Leader {
				return node
			}
		}

		node.Close()
		t.Fatal("node did not become leader")
		return nil
	}

	node := openLeader()
	handler := statusHandler(node)

	put := `{"value":"Go","client_id":"client-1","request_id":"put-1"}`
	rec := sendKVRequest(t, handler, http.MethodPut, "/kv/language", put)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT failed: %d %s", rec.Code, rec.Body.String())
	}

	cas := `{"expected":"Go","value":"Python","client_id":"client-1","request_id":"cas-1"}`
	rec = sendKVRequest(t, handler, http.MethodPost, "/kv/language/cas", cas)
	if rec.Code != http.StatusOK {
		t.Fatalf("CAS failed: %d %s", rec.Code, rec.Body.String())
	}

	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := openLeader()
	defer restarted.Close()
	handler = statusHandler(restarted)

	rec = sendKVRequest(t, handler, http.MethodGet, "/kv/language", "")
	if rec.Code != http.StatusOK ||
		rec.Body.String() != "{\"key\":\"language\",\"value\":\"Python\"}\n" {
		t.Fatalf("KV state not recovered: %d %s", rec.Code, rec.Body.String())
	}

	// An identical retry must return the original result.
	rec = sendKVRequest(t, handler, http.MethodPost, "/kv/language/cas", cas)
	if rec.Code != http.StatusOK ||
		rec.Body.String() != "{\"applied\":true,\"value\":\"Python\"}\n" {
		t.Fatalf("dedup result not recovered: %d %s", rec.Code, rec.Body.String())
	}

	// A conflicting retry must remain rejected after restart.
	conflict := `{"expected":"Go","value":"Java","client_id":"client-1","request_id":"cas-1"}`
	rec = sendKVRequest(t, handler, http.MethodPost, "/kv/language/cas", conflict)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected conflict after restart, got %d: %s",
			rec.Code, rec.Body.String())
	}
}
