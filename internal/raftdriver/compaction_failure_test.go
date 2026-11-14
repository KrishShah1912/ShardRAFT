package raftdriver

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

// newFailureTestNode creates a single-node cluster
// with a committed and applied leader no-op.
func newFailureTestNode(
	t *testing.T,
) (*raft.Raft, *wal.WAL, *Driver, string) {
	t.Helper()

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

	t.Cleanup(func() {
		_ = storage.Close()
	})

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	return node, storage, driver, snapPath
}

func TestCompactionStopsOnSnapshotSaveFailure(t *testing.T) {
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
	defer storage.Close()

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	// A directory at the snapshot destination
	// prevents atomic replacement with a file.
	if err := os.Mkdir(snapPath, 0755); err != nil {
		t.Fatal(err)
	}

	err = driver.CompactKV(
		node, storage, store, snapPath,
	)

	if err == nil {
		t.Fatal("expected snapshot save failure")
	}

	if !driver.Stopped() {
		t.Fatal("driver must stop after snapshot failure")
	}

	if err := driver.Process(); !errors.Is(
		err, ErrStopped,
	) {
		t.Fatalf(
			"expected ErrStopped, got %v", err,
		)
	}

	// Snapshot persistence failed before WAL
	// compaction, so the boundary must not advance.
	if node.SnapshotMetadata().LastIncludedIndex != 0 {
		t.Fatal("in-memory boundary advanced unexpectedly")
	}

	state, err := storage.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if state.Snapshot.LastIncludedIndex != 0 {
		t.Fatal("WAL boundary advanced unexpectedly")
	}
}

func TestClosedWALStopsDriverBeforeCompaction(t *testing.T) {
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

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// CompactKV calls Process first. A closed WAL
	// must prevent the operation from proceeding.
	err = driver.CompactKV(
		node, storage, store, snapPath,
	)

	if err == nil {
		t.Fatal("expected closed WAL failure")
	}

	if !driver.Stopped() {
		t.Fatal("driver must stop after WAL failure")
	}

	if err := driver.Process(); !errors.Is(
		err, ErrStopped,
	) {
		t.Fatalf(
			"expected ErrStopped, got %v", err,
		)
	}

	if node.SnapshotMetadata().LastIncludedIndex != 0 {
		t.Fatal("Raft boundary changed unexpectedly")
	}

	if _, err := os.Stat(snapPath); !errors.Is(
		err, os.ErrNotExist,
	) {
		t.Fatalf(
			"snapshot should not have been created: %v",
			err,
		)
	}
}

func TestCorruptSnapshotRecoveryRejected(t *testing.T) {
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

	if err := driver.CompactKV(
		node, storage, store, snapPath,
	); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Replace the snapshot with invalid data.
	if err := os.WriteFile(
		snapPath,
		[]byte("invalid snapshot contents"),
		0644,
	); err != nil {
		t.Fatal(err)
	}

	_, reopened, _, _, err := RecoverKVDriver(
		walPath, snapPath, 1, nil, 5, send,
	)

	if reopened != nil {
		_ = reopened.Close()
		t.Fatal("failed recovery returned an open WAL")
	}

	if !errors.Is(err, snapshot.ErrCorrupt) {
		t.Fatalf(
			"expected corrupt snapshot error, got %v",
			err,
		)
	}
}
