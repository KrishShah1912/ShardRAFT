package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExplicitCheckpointRetiresObsoleteSegments(t *testing.T) {
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

	expected := copyState(w.state)

	// Confirm the published checkpoint is valid before cleanup.
	if err := w.VerifyPublishedCheckpoint(); err != nil {
		t.Fatal(err)
	}

	if err := w.RetireObsoleteSegments(); err != nil {
		t.Fatal(err)
	}

	// No obsolete WAL segments should remain.
	entries, err := os.ReadDir(segmentDirectory(path))
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatalf("unexpected directory: %s", entry.Name())
		}
	}

	numbers, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(numbers) == 0 {
		t.Fatal("no active WAL segments remain")
	}

	if numbers[0] != manifest.FirstSegment {
		t.Fatalf(
			"first segment = %d, want %d",
			numbers[0],
			manifest.FirstSegment,
		)
	}

	// The checkpoint must remain available.
	if _, err := os.Stat(
		checkpointPath(path, manifest.CheckpointSegment),
	); err != nil {
		t.Fatalf("published checkpoint missing: %v", err)
	}

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
		t.Fatal("state mismatch after segment retirement")
	}
}
