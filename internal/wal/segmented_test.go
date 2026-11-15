package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSegmentedCreatesFirstSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if w.number != 1 {
		t.Fatalf("segment number = %d, want 1", w.number)
	}

	if _, err := os.Stat(segmentPath(path, 1)); err != nil {
		t.Fatal(err)
	}
}

func TestOpenSegmentedReopensLastSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if reopened.number != 1 {
		t.Fatalf(
			"reopened segment = %d, want 1",
			reopened.number,
		)
	}
}

func TestOpenSegmentedRejectsInvalidSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	_, err := OpenSegmented(
		path,
		SegmentOptions{MaxSegmentSize: 4},
	)
	if err == nil {
		t.Fatal("expected invalid segment size error")
	}
}
