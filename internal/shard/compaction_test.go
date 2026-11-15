package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestShardCompactionSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	snapshotPath := path + ".snapshot"

	s := openTestShard(t, path)
	electTestLeader(t, s)

	for i := 0; i < 10; i++ {
		err := s.Propose(kvstore.Command{
			Op:        kvstore.OpPut,
			Key:       "key",
			Value:     "value",
			ClientID:  "client",
			RequestID: string(rune('a' + i)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	before, err := s.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(snapshotPath); err != nil {
		t.Fatal(err)
	}

	snap, err := snapshot.Load(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}

	if snap.AppliedIndex != before {
		t.Fatalf(
			"snapshot index = %d, want %d",
			snap.AppliedIndex, before,
		)
	}

	if len(snap.Dedup) != 10 {
		t.Fatalf(
			"dedup records = %d, want 10",
			len(snap.Dedup),
		)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := openTestShard(t, path)
	defer restarted.Close()

	value, exists, err := restarted.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "value" {
		t.Fatalf(
			"recovered value = %q, exists=%v",
			value, exists,
		)
	}

	after, err := restarted.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf(
			"recovered applied index = %d, want %d",
			after, before,
		)
	}
}

func TestShardRejectsWrongCompactionSnapshotPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	s := openTestShard(t, path)
	defer s.Close()

	electTestLeader(t, s)

	err := s.Compact(path + ".wrong-snapshot")
	if err == nil {
		t.Fatal("expected snapshot-path mismatch rejection")
	}
}

func TestShardCompactionRequiresAppliedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	s := openTestShard(t, path)
	defer s.Close()

	err := s.Compact(path + ".snapshot")

	if err == nil {
		t.Fatal("expected compaction rejection without applied entries")
	}
	if errors.Is(err, ErrStopped) {
		t.Fatal("precondition failure must not stop the shard")
	}
}
