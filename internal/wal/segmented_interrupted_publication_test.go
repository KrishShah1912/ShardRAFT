package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestInterruptedExplicitCheckpointPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	// Establish an existing published generation.
	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	previousManifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("previous manifest missing")
	}

	expected := copyState(w.state)

	// Prepare a second checkpoint without publishing it.
	temp, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}

	tempPath := temp.Name()
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}

	nextNumber := w.number + 1

	if err := os.Rename(
		tempPath,
		checkpointPath(path, nextNumber),
	); err != nil {
		t.Fatal(err)
	}

	next, err := os.OpenFile(
		segmentPath(path, nextNumber),
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0644,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := next.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}

	if err := defaultPhysicalCompactionOps().syncDir(
		segmentDirectory(path),
	); err != nil {
		t.Fatal(err)
	}

	// Simulate a process crash before MANIFEST changes.
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
		t.Fatal("interrupted publication changed recovered state")
	}

	currentManifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("manifest disappeared")
	}

	if currentManifest != previousManifest {
		t.Fatalf(
			"unpublished checkpoint changed manifest: %+v -> %+v",
			previousManifest,
			currentManifest,
		)
	}

	if reopened.number != nextNumber {
		t.Fatalf(
			"active segment = %d, want %d",
			reopened.number,
			nextNumber,
		)
	}
}
