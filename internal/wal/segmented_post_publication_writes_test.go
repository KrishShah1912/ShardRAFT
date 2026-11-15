package wal

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestWritesSurviveExplicitCheckpointRetirement(t *testing.T) {
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

	// Retire the old WAL generation.
	if err := w.RetireObsoleteSegments(); err != nil {
		t.Fatal(err)
	}

	// Write new persistent Raft state after retirement.
	for i := 0; i < 10; i++ {
		hs := w.state.HardState
		hs.Term++

		if err := w.SaveHardState(hs); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
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
		t.Fatal("post-publication writes were not recovered")
	}

	if err := reopened.VerifyPublishedCheckpoint(); err != nil {
		t.Fatal(err)
	}
}
