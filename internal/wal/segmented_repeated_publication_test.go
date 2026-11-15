package wal

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestRepeatedExplicitCheckpointPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	first, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || first.CheckpointSegment == 0 {
		t.Fatal("first explicit checkpoint not published")
	}

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	second, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("second manifest missing")
	}

	if second.CheckpointSegment <= first.CheckpointSegment {
		t.Fatalf(
			"checkpoint did not advance: %d -> %d",
			first.CheckpointSegment,
			second.CheckpointSegment,
		)
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
		t.Fatal("state mismatch after repeated publication")
	}

	if err := reopened.VerifyPublishedCheckpoint(); err != nil {
		t.Fatal(err)
	}
}
