package wal

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExplicitCheckpointManifestSyncFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)
	expected := copyState(w.state)

	injected := errors.New("injected manifest directory sync failure")

	ops := defaultPhysicalCompactionOps()
	realSyncDir := ops.syncDir

	ops.syncDir = func(dir string) error {
		// Only fail after MANIFEST has been renamed.
		if _, err := os.Stat(filepath.Join(dir, "MANIFEST")); err == nil {
			return injected
		}
		return realSyncDir(dir)
	}

	w.manifestPublishOps = &ops

	err := w.PublishCheckpoint()
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	if !w.unusable {
		t.Fatal("WAL remains usable after manifest sync failure")
	}

	if err := w.Sync(); !errors.Is(err, ErrUnusable) {
		t.Fatalf("expected ErrUnusable, got %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || manifest.CheckpointSegment == 0 {
		t.Fatal("renamed manifest does not reference checkpoint")
	}

	if _, err := os.Stat(
		checkpointPath(path, manifest.CheckpointSegment),
	); err != nil {
		t.Fatalf("published checkpoint missing: %v", err)
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
		t.Fatal("state mismatch after manifest sync failure")
	}

	if err := reopened.VerifyPublishedCheckpoint(); err != nil {
		t.Fatal(err)
	}
}
