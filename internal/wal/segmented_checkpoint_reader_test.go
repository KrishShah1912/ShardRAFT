package wal

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReadCheckpointFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w := createCrashRecoveryWAL(
		t,
		path,
		SegmentOptions{MaxSegmentSize: 256},
	)
	defer w.Close()

	expected := copyState(w.state)

	f, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}

	checkpointFile := f.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(checkpointFile)

	actual, err := readCheckpointFile(checkpointFile)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		copyState(actual),
		copyState(expected),
	) {
		t.Fatal("checkpoint state mismatch")
	}
}

func TestReadCheckpointFileRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.chk")

	if err := os.WriteFile(
		path,
		[]byte("invalid checkpoint"),
		0644,
	); err != nil {
		t.Fatal(err)
	}

	_, err := readCheckpointFile(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestReadCheckpointFileRejectsTrailingData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w := createCrashRecoveryWAL(
		t,
		path,
		SegmentOptions{MaxSegmentSize: 256},
	)
	defer w.Close()

	f, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}

	checkpointFile := f.Name()

	if _, err := f.WriteAt(
		[]byte("extra"),
		func() int64 {
			info, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}
			return info.Size()
		}(),
	); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(checkpointFile)

	_, err = readCheckpointFile(checkpointFile)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}
