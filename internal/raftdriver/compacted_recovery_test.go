package raftdriver

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestCompactedKVRecoveryEndToEnd(t *testing.T) {
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

	propose("x", "10")

	// Snapshot includes the leader no-op and x=10.
	snapshotIndex := store.AppliedIndex()

	term, ok := node.TermAt(snapshotIndex)
	if !ok {
		t.Fatal("snapshot boundary term unavailable")
	}

	s := snapshot.New(
		snapshotIndex,
		term,
		store.Snapshot(),
	)

	if err := snapshot.Save(snapPath, s); err != nil {
		t.Fatal(err)
	}

	// Verify the saved snapshot before compaction.
	verified, err := snapshot.Load(snapPath)
	if err != nil {
		t.Fatal(err)
	}

	if verified.AppliedIndex != snapshotIndex ||
		verified.AppliedTerm != term {
		t.Fatal("saved snapshot boundary mismatch")
	}

	// Append another committed command after snapshot.
	propose("y", "20")

	// Compact the persisted WAL through the snapshot.
	// The running Raft node is not compacted here.
	// Close and recover immediately afterward.
	if err := storage.CompactPrefix(
		snapshotIndex, term,
	); err != nil {
		t.Fatal(err)
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

	if restored.Status().Role != raft.Follower {
		t.Fatal("expected recovered follower")
	}

	base := restored.SnapshotMetadata()

	if base.LastIncludedIndex != snapshotIndex ||
		base.LastIncludedTerm != term {
		t.Fatalf(
			"unexpected recovered snapshot boundary: %+v",
			base,
		)
	}

	if recoveredStore.AppliedIndex() != snapshotIndex {
		t.Fatalf(
			"expected restored KV index %d, got %d",
			snapshotIndex,
			recoveredStore.AppliedIndex(),
		)
	}

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	for key, expected := range map[string]string{
		"x": "10",
		"y": "20",
	} {
		value, exists := recoveredStore.Get(key)

		if !exists || value != expected {
			t.Fatalf(
				"%s: expected %q, got %q",
				key, expected, value,
			)
		}
	}

	if recoveredStore.AppliedIndex() != 3 {
		t.Fatalf(
			"expected applied index 3, got %d",
			recoveredStore.AppliedIndex(),
		)
	}

	// A recovered node must continue using absolute
	// indexes for new proposals.
	for i := 0; i < 5; i++ {
		restored.Tick()
	}

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	data, err := kvstore.EncodeCommand(kvstore.Command{
		Op:    kvstore.OpSet,
		Key:   "z",
		Value: "30",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := restored.Propose(data); err != nil {
		t.Fatal(err)
	}

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	value, exists := recoveredStore.Get("z")
	if !exists || value != "30" {
		t.Fatalf("expected z=30, got %q", value)
	}

	if recoveredStore.AppliedIndex() != 5 {
		t.Fatalf(
			"expected applied index 5, got %d",
			recoveredStore.AppliedIndex(),
		)
	}
}

func TestCompactedRecoveryRequiresSnapshot(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "node.wal")
	snapPath := filepath.Join(dir, "missing.snapshot")

	send := func([]raft.Message) error { return nil }

	node, storage, driver, _, err := RecoverKVDriver(
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

	if err := storage.CompactPrefix(1, 1); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, _, _, err = RecoverKVDriver(
		walPath, snapPath, 1, nil, 5, send,
	)

	if err == nil {
		t.Fatal("expected missing snapshot rejection")
	}

	if !strings.Contains(err.Error(), "requires a matching snapshot") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCompactedRecoveryRejectsMismatchedSnapshot(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "node.wal")
	snapPath := filepath.Join(dir, "state.snapshot")

	send := func([]raft.Message) error { return nil }

	node, storage, driver, _, err := RecoverKVDriver(
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

	if err := storage.CompactPrefix(1, 1); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	bad := snapshot.New(
		1, 99, map[string]string{"x": "wrong"},
	)

	if err := snapshot.Save(snapPath, bad); err != nil {
		t.Fatal(err)
	}

	_, _, _, _, err = RecoverKVDriver(
		walPath, snapPath, 1, nil, 5, send,
	)

	if err == nil {
		t.Fatal("expected snapshot boundary mismatch")
	}
}
