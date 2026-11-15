package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSegmentedRecoveryIgnoresUnpublishedCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)
	expected := copyState(w.state)

	// Prepare a valid checkpoint but do not publish
	// a manifest referencing it.
	temp, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}

	tempPath := temp.Name()

	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash after the checkpoint segment
	// rename but before manifest publication.
	orphanPath := segmentPath(path, w.number+1)

	if err := os.Rename(tempPath, orphanPath); err != nil {
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
		t.Fatalf(
			"unpublished checkpoint broke recovery: %v",
			err,
		)
	}

	if !reflect.DeepEqual(
		copyState(actual),
		copyState(expected),
	) {
		t.Fatalf(
			"recovered wrong generation:\nexpected=%+v\nactual=%+v",
			expected,
			actual,
		)
	}
}
