package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

func TestHTTPStatusEndpoint(t *testing.T) {
	node, err := shard.Open(shard.Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         filepath.Join(t.TempDir(), "wal"),
		Send: func([]raft.Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	for i := 0; i < 20; i++ {
		if err := node.Tick(); err != nil {
			t.Fatal(err)
		}

		status, err := node.Status()
		if err != nil {
			t.Fatal(err)
		}
		if status.Role == raft.Leader {
			break
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()

	statusHandler(node).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d, body=%s",
			rec.Code, rec.Body.String())
	}

	var result struct {
		ID           uint64 `json:"id"`
		Role         uint64 `json:"role"`
		Term         uint64 `json:"term"`
		LeaderID     uint64 `json:"leader_id"`
		CommitIndex  uint64 `json:"commit_index"`
		AppliedIndex uint64 `json:"applied_index"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if result.ID != 1 {
		t.Fatalf("node ID = %d, want 1", result.ID)
	}

	if result.Role != uint64(raft.Leader) {
		t.Fatalf("role = %d, want leader", result.Role)
	}

	if result.LeaderID != 1 {
		t.Fatalf("leader ID = %d, want 1", result.LeaderID)
	}

	if result.Term == 0 {
		t.Fatal("expected nonzero term")
	}

	if result.CommitIndex != result.AppliedIndex {
		t.Fatalf("commit=%d applied=%d",
			result.CommitIndex, result.AppliedIndex)
	}
}

func TestHTTPStatusAfterShardClose(t *testing.T) {
	node, err := shard.Open(shard.Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         filepath.Join(t.TempDir(), "wal"),
		Send: func([]raft.Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()

	statusHandler(node).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP status = %d, want 503",
			rec.Code)
	}
}
