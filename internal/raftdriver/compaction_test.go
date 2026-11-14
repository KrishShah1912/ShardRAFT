package raftdriver

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestCoordinatedKVCompaction(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "node.wal")
	snapPath := filepath.Join(dir, "state.snapshot")

	send := func([]raft.Message) error { return nil }

	node, storage, driver, store, err := RecoverKVDriver(
		walPath, snapPath, 1, nil, 5, send,
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	data, err := kvstore.EncodeCommand(kvstore.Command{
		Op:    kvstore.OpSet,
		Key:   "alpha",
		Value: "100",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.Propose(data); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if err := driver.CompactKV(
		node, storage, store, snapPath,
	); err != nil {
		t.Fatal(err)
	}

	base := node.SnapshotMetadata()

	if base.LastIncludedIndex != 2 {
		t.Fatalf(
			"expected compacted index 2, got %d",
			base.LastIncludedIndex,
		)
	}

	s, err := snapshot.Load(snapPath)
	if err != nil {
		t.Fatal(err)
	}

	if s.AppliedIndex != 2 {
		t.Fatal("incorrect snapshot applied index")
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	restored, reopened, recoveredDriver, recoveredStore, err :=
		RecoverKVDriver(
			walPath, snapPath, 1, nil, 5, send,
		)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	value, exists := recoveredStore.Get("alpha")

	if !exists || value != "100" {
		t.Fatalf(
			"expected alpha=100, got %q", value,
		)
	}

	if restored.SnapshotMetadata().LastIncludedIndex != 2 {
		t.Fatal("compaction boundary not recovered")
	}
}

func TestRecoveryAcceptsSnapshotAheadOfWALBoundary(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "node.wal")
	snapPath := filepath.Join(dir, "state.snapshot")

	send := func([]raft.Message) error { return nil }

	node, storage, driver, store, err := RecoverKVDriver(
		walPath, snapPath, 1, nil, 5, send,
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	// Snapshot the committed leader no-op at index 1.
	index := store.AppliedIndex()
	term, ok := node.TermAt(index)

	if !ok {
		t.Fatal("missing snapshot boundary term")
	}

	if err := snapshot.Save(
		snapPath,
		snapshot.New(index, term, store.Snapshot()),
	); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash before WAL compaction.
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	_, reopened, recoveredDriver, recoveredStore, err :=
		RecoverKVDriver(
			walPath, snapPath, 1, nil, 5, send,
		)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	if recoveredStore.AppliedIndex() != index {
		t.Fatalf(
			"expected applied index %d, got %d",
			index,
			recoveredStore.AppliedIndex(),
		)
	}
}
