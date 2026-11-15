package shard

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestRepeatedCompactionPreservesState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	snapshotPath := path + ".snapshot"

	s := openTestShard(t, path)
	electTestLeader(t, s)

	first := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "first",
		Value:     "one",
		ClientID:  "client",
		RequestID: "request-1",
	}

	if err := s.Propose(first); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(snapshotPath); err != nil {
		t.Fatalf("first compaction: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTestShard(t, path)
	electTestLeader(t, s)

	second := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "second",
		Value:     "two",
		ClientID:  "client",
		RequestID: "request-2",
	}

	if err := s.Propose(second); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(snapshotPath); err != nil {
		t.Fatalf("second compaction: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTestShard(t, path)
	defer s.Close()

	for key, expected := range map[string]string{
		"first":  "one",
		"second": "two",
	} {
		value, exists, err := s.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !exists || value != expected {
			t.Fatalf(
				"key %q: value=%q exists=%v, want %q",
				key, value, exists, expected,
			)
		}
	}

	snap, err := snapshot.Load(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}

	if len(snap.Dedup) != 2 {
		t.Fatalf(
			"dedup records = %d, want 2",
			len(snap.Dedup),
		)
	}
}
