package raftdriver

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestRepeatedKVCompactionAndRecovery(t *testing.T) {
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

	propose := func(key, value string) {
		t.Helper()

		data, err := kvstore.EncodeCommand(kvstore.Command{
			Op:    kvstore.OpSet,
			Key:   key,
			Value: value,
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
	}

	propose("alpha", "100")

	// First compaction: entries 1 and 2.
	if err := driver.CompactKV(
		node, storage, store, snapPath,
	); err != nil {
		t.Fatal(err)
	}

	if node.SnapshotMetadata().LastIncludedIndex != 2 {
		t.Fatal("first compaction boundary should be 2")
	}

	propose("beta", "200")
	propose("gamma", "300")

	// Second compaction: through entry 4.
	if err := driver.CompactKV(
		node, storage, store, snapPath,
	); err != nil {
		t.Fatal(err)
	}

	if node.SnapshotMetadata().LastIncludedIndex != 4 {
		t.Fatal("second compaction boundary should be 4")
	}

	// An additional entry must remain in the WAL.
	propose("delta", "400")

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

	if restored.SnapshotMetadata().LastIncludedIndex != 4 {
		t.Fatal("second compaction boundary not recovered")
	}

	if recoveredStore.AppliedIndex() != 4 {
		t.Fatalf(
			"expected snapshot index 4, got %d",
			recoveredStore.AppliedIndex(),
		)
	}

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	expected := map[string]string{
		"alpha": "100",
		"beta":  "200",
		"gamma": "300",
		"delta": "400",
	}

	for key, want := range expected {
		got, ok := recoveredStore.Get(key)
		if !ok || got != want {
			t.Fatalf(
				"%s: expected %q, got %q",
				key, want, got,
			)
		}
	}

	if recoveredStore.AppliedIndex() != 5 {
		t.Fatalf(
			"expected applied index 5, got %d",
			recoveredStore.AppliedIndex(),
		)
	}
}

func TestCrashBetweenSecondSnapshotAndWALCompaction(t *testing.T) {
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

	// First committed command.
	first, err := kvstore.EncodeCommand(kvstore.Command{
		Op:    kvstore.OpSet,
		Key:   "first",
		Value: "one",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.Propose(first); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	// Complete the first compaction.
	if err := driver.CompactKV(
		node, storage, store, snapPath,
	); err != nil {
		t.Fatal(err)
	}

	// Commit another command.
	second, err := kvstore.EncodeCommand(kvstore.Command{
		Op:    kvstore.OpSet,
		Key:   "second",
		Value: "two",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := node.Propose(second); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	// Save a newer snapshot, but deliberately
	// do not advance the WAL compaction boundary.
	index := store.AppliedIndex()
	term, ok := node.TermAt(index)
	if !ok {
		t.Fatal("missing term at snapshot boundary")
	}

	if err := snapshot.Save(
		snapPath,
		snapshot.New(index, term, store.Snapshot()),
	); err != nil {
		t.Fatal(err)
	}

	// Simulate shutdown after the snapshot write.
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

	// WAL boundary should still be the first snapshot.
	if restored.SnapshotMetadata().LastIncludedIndex != 2 {
		t.Fatal("expected old WAL compaction boundary")
	}

	// KV state should come from the newer snapshot.
	if recoveredStore.AppliedIndex() != 3 {
		t.Fatalf(
			"expected snapshot index 3, got %d",
			recoveredStore.AppliedIndex(),
		)
	}

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	for key, want := range map[string]string{
		"first":  "one",
		"second": "two",
	} {
		got, ok := recoveredStore.Get(key)
		if !ok || got != want {
			t.Fatalf(
				"%s: expected %q, got %q",
				key, want, got,
			)
		}
	}
}
