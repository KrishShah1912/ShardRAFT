package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryRejectsCorruptPublishedCheckpoint(t *testing.T) {
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

	checkpointFile := checkpointPath(
		path,
		manifest.CheckpointSegment,
	)

	// Corrupt the published checkpoint without changing MANIFEST.
	f, err := os.OpenFile(checkpointFile, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.WriteAt([]byte{0xFF}, headerSize); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
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

	// Failed recovery must not allow subsequent writes.
	if !reopened.unusable {
		t.Fatal("WAL remains usable after failed recovery")
	}
}
