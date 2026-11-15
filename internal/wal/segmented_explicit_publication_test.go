package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPublishCheckpointUsesExplicitCheckpointFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)
	expected := copyState(w.state)

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("published manifest missing")
	}

	if manifest.CheckpointSegment == 0 {
		t.Fatal("manifest has no explicit checkpoint reference")
	}

	checkpointFile := checkpointPath(
		path,
		manifest.CheckpointSegment,
	)

	if _, err := os.Stat(checkpointFile); err != nil {
		t.Fatalf("published checkpoint missing: %v", err)
	}

	actual, err := readCheckpointFile(checkpointFile)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		copyState(actual),
		copyState(expected),
	) {
		t.Fatal("published checkpoint state mismatch")
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	recovered, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		copyState(recovered),
		copyState(expected),
	) {
		t.Fatal("recovery did not load published checkpoint")
	}
}
