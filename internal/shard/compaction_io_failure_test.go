package shard

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
)

func TestSnapshotSaveFailurePreservesShard(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.wal")
	snapshotPath := path + ".snapshot"

	s := openTestShard(t, path)
	electTestLeader(t, s)

	if err := s.Propose(kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "durable",
		Value:     "original",
		ClientID:  "client",
		RequestID: "request-1",
	}); err != nil {
		t.Fatal(err)
	}

	before, err := s.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Force snapshot.Save to fail without depending on
	// filesystem permissions or the current OS user.
	if err := os.Mkdir(snapshotPath, 0700); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(snapshotPath); err == nil {
		t.Fatal("expected snapshot save failure")
	}

	// A failed snapshot save must not compact Raft/WAL.
	if base := s.node.SnapshotMetadata(); base.LastIncludedIndex != 0 {
		t.Fatalf(
			"Raft compacted after failed snapshot save: %+v",
			base,
		)
	}

	after, err := s.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("applied index changed: %d -> %d", before, after)
	}

	value, exists, err := s.Get("durable")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "original" {
		t.Fatalf("value = %q, exists=%v", value, exists)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The original WAL must remain recoverable.
	if err := os.Remove(snapshotPath); err != nil {
		t.Fatal(err)
	}

	restarted := openTestShard(t, path)
	defer restarted.Close()

	value, exists, err = restarted.Get("durable")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "original" {
		t.Fatalf(
			"recovered value = %q, exists=%v",
			value, exists,
		)
	}
}

func TestCompactionPreconditionFailureKeepsShardUsable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)
	defer s.Close()

	err := s.Compact(path + ".snapshot")
	if err == nil {
		t.Fatal("expected no-applied-entries error")
	}
	if errors.Is(err, ErrStopped) {
		t.Fatalf("unexpected stopped state: %v", err)
	}

	if err := s.Tick(); err != nil {
		t.Fatalf("Tick after precondition failure: %v", err)
	}
}
