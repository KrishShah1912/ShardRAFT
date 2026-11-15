package shard

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestRecoveryAfterInterruptedCheckpointPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	snapshotPath := path + ".snapshot"

	s := openTestShard(t, path)
	electTestLeader(t, s)

	if err := s.Propose(kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "account",
		Value:     "original",
		ClientID:  "client",
		RequestID: "request-1",
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.driver.Process(); err != nil {
		t.Fatal(err)
	}

	index, data, dedup := s.store.CaptureSnapshot()
	term, ok := s.node.TermAt(index)
	if !ok || term == 0 {
		t.Fatal("missing snapshot boundary term")
	}

	// Simulate compaction stopping after the durable
	// snapshot and logical WAL compaction, but before
	// PublishCheckpoint.
	if err := snapshot.Save(
		snapshotPath,
		snapshot.NewWithDedup(index, term, data, dedup),
	); err != nil {
		t.Fatal(err)
	}

	if err := s.storage.CompactPrefix(index, term); err != nil {
		t.Fatal(err)
	}

	if err := s.node.CompactPrefix(index, term); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := openTestShard(t, path)
	defer restarted.Close()

	value, exists, err := restarted.Get("account")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "original" {
		t.Fatalf(
			"recovered value = %q, exists=%v",
			value, exists,
		)
	}

	if err := restarted.Compact(snapshotPath); err != nil {
		t.Fatalf(
			"compaction after interrupted publication: %v",
			err,
		)
	}
}

func TestRecoveryAfterSnapshotBeforeWALCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	snapshotPath := path + ".snapshot"

	s := openTestShard(t, path)
	electTestLeader(t, s)

	if err := s.Propose(kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "phase",
		Value:     "snapshot-saved",
		ClientID:  "client",
		RequestID: "request-1",
	}); err != nil {
		t.Fatal(err)
	}

	index, data, dedup := s.store.CaptureSnapshot()
	term, ok := s.node.TermAt(index)
	if !ok || term == 0 {
		t.Fatal("missing snapshot boundary term")
	}

	// Simulate a crash after snapshot.Save, before
	// WAL.CompactPrefix.
	if err := snapshot.Save(
		snapshotPath,
		snapshot.NewWithDedup(index, term, data, dedup),
	); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := openTestShard(t, path)
	defer restarted.Close()

	value, exists, err := restarted.Get("phase")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "snapshot-saved" {
		t.Fatalf(
			"recovered value = %q, exists=%v",
			value, exists,
		)
	}
}
