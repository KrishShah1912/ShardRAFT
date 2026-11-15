package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryRejectsMissingPublishedCheckpoint(t *testing.T) {
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

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Remove the published checkpoint while retaining
	// its MANIFEST reference.
	if err := os.Remove(
		checkpointPath(path, manifest.CheckpointSegment),
	); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	_, err = reopened.Recover()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}

	if !reopened.unusable {
		t.Fatal("WAL remains usable after failed recovery")
	}
}
