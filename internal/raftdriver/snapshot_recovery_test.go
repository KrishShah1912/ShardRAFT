package raftdriver

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestSnapshotAwareKVRecovery(t *testing.T) {
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
			Op: kvstore.OpSet, Key: key, Value: value,
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

	// Entries 1 and 2 are committed.
	// Save a snapshot at the second entry.
	s := snapshot.New(
		store.AppliedIndex(),
		node.Status().Term,
		store.Snapshot(),
	)
	if err := snapshot.Save(snapPath, s); err != nil {
		t.Fatal(err)
	}

	// Entry 3 is committed after the snapshot.
	propose("y", "20")

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

	// The snapshot already includes entries 1 and 2.
	if recoveredStore.AppliedIndex() != 2 {
		t.Fatalf(
			"expected snapshot index 2, got %d",
			recoveredStore.AppliedIndex(),
		)
	}

	// Process replays the remaining committed entry.
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
}

func TestSnapshotRecoveryRejectsMismatchedTerm(t *testing.T) {
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

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Index 1 exists, but its actual term is 1.
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
		t.Fatal("expected snapshot/WAL mismatch error")
	}
}
