package wal

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestExplicitCheckpointRecoveryAfterRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || manifest.CheckpointSegment == 0 {
		t.Fatal("explicit checkpoint manifest missing")
	}

	firstSegment := w.number

	// Repeated hard-state records force WAL rotation.
	// Each update must remain valid for Raft.
	for i := 0; i < 20; i++ {
		hs := w.state.HardState
		hs.Term++

		if err := w.SaveHardState(hs); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if w.number <= firstSegment {
		t.Fatal("WAL did not rotate after checkpoint")
	}

	expected := copyState(w.state)

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	actual, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		copyState(actual),
		copyState(expected),
	) {
		t.Fatal("recovered state mismatch after WAL rotation")
	}

	if err := reopened.VerifyPublishedCheckpoint(); err != nil {
		t.Fatal(err)
	}

	// Ensure the recovered state still represents
	// a valid Raft persistent-state value.
	var _ raft.PersistentState = actual
}
