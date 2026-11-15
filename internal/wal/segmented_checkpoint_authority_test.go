package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestUnpublishedCheckpointFileDoesNotChangeRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)
	expected := copyState(w.state)

	temp, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}

	tempPath := temp.Name()
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}

	candidateNumber := w.number + 1
	candidatePath := checkpointPath(path, candidateNumber)

	if err := os.Rename(tempPath, candidatePath); err != nil {
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
	defer reopened.Close()

	actual, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		copyState(actual),
		copyState(expected),
	) {
		t.Fatal("unpublished checkpoint changed recovery state")
	}

	if reopened.number == candidateNumber {
		t.Fatal("unpublished checkpoint selected as active writer")
	}

	// Recovery must not publish the checkpoint implicitly.
	_, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}

	if found {
		t.Fatal("recovery implicitly published checkpoint")
	}
}
