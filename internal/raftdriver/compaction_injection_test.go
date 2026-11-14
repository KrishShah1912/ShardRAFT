package raftdriver

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestInjectedWALCompactionFailure(t *testing.T) {
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
		Key:   "failure-test",
		Value: "preserved",
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

	injectedErr := errors.New("injected WAL compaction failure")
	called := false

	err = driver.compactKVWith(
		node,
		storage,
		store,
		snapPath,
		func(index, term uint64) error {
			called = true

			if index != 2 || term != 1 {
				t.Fatalf(
					"unexpected compaction boundary: %d/%d",
					index, term,
				)
			}

			return injectedErr
		},
	)

	if !called {
		t.Fatal("WAL compaction callback was not called")
	}

	if !errors.Is(err, injectedErr) {
		t.Fatalf(
			"expected injected error, got %v",
			err,
		)
	}

	if !driver.Stopped() {
		t.Fatal("driver should stop after compaction failure")
	}

	if err := driver.Process(); !errors.Is(
		err, ErrStopped,
	) {
		t.Fatalf(
			"expected ErrStopped, got %v",
			err,
		)
	}

	// The snapshot must have been persisted
	// before the injected failure.
	s, err := snapshot.Load(snapPath)
	if err != nil {
		t.Fatal(err)
	}

	if s.AppliedIndex != 2 || s.AppliedTerm != 1 {
		t.Fatalf(
			"unexpected saved snapshot: index=%d term=%d",
			s.AppliedIndex, s.AppliedTerm,
		)
	}

	// In-memory Raft compaction must not happen
	// after a WAL compaction failure.
	if node.SnapshotMetadata().LastIncludedIndex != 0 {
		t.Fatal("in-memory Raft boundary advanced")
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// The injected callback did not modify the WAL.
	// Recovery must accept the newer snapshot and
	// replay any remaining committed entries.
	restored, reopened, recoveredDriver, recoveredStore, err :=
		RecoverKVDriver(
			walPath, snapPath, 1, nil, 5, send,
		)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if restored.SnapshotMetadata().LastIncludedIndex != 0 {
		t.Fatal("WAL compaction boundary unexpectedly advanced")
	}

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	value, exists := recoveredStore.Get("failure-test")
	if !exists || value != "preserved" {
		t.Fatalf(
			"expected preserved value, got %q",
			value,
		)
	}

	if recoveredStore.AppliedIndex() != 2 {
		t.Fatalf(
			"expected applied index 2, got %d",
			recoveredStore.AppliedIndex(),
		)
	}
}
