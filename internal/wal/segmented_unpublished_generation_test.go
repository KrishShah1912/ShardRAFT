package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSegmentedRecoveryPreservesPublishedGeneration(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	originalManifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("published manifest missing")
	}

	expected := copyState(w.state)

	// Prepare another checkpoint, but deliberately
	// do not publish a new manifest.
	temp, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}

	tempPath := temp.Name()
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}

	orphanNumber := w.number + 1

	if err := os.Rename(
		tempPath,
		segmentPath(path, orphanNumber),
	); err != nil {
		t.Fatal(err)
	}

	if err := defaultPhysicalCompactionOps().syncDir(
		segmentDirectory(path),
	); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}

	actual, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		copyState(actual),
		copyState(expected),
	) {
		t.Fatal("recovery changed the published Raft state")
	}

	if reopened.number == orphanNumber {
		t.Fatal("unpublished checkpoint selected as active writer")
	}

	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || manifest != originalManifest {
		t.Fatal("published manifest changed during recovery")
	}
}
