package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func createSegmentedWALForCorruptionTest(
	t *testing.T,
) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(
		path,
		SegmentOptions{MaxSegmentSize: 256},
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 10; i++ {
		err := w.AppendEntries([]raft.Entry{{
			Index: i,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  []byte("persisted data"),
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return path
}

func expectSegmentedCorruption(
	t *testing.T,
	path string,
) {
	t.Helper()

	w, err := OpenSegmented(
		path,
		SegmentOptions{MaxSegmentSize: 256},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := w.Recover(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestSegmentedWALRejectsChecksumCorruption(t *testing.T) {
	path := createSegmentedWALForCorruptionTest(t)

	f, err := os.OpenFile(
		segmentPath(path, 1),
		os.O_RDWR,
		0644,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Flip one byte in the first record payload.
	offset := int64(headerSize + 2)

	data := []byte{0}
	if _, err := f.ReadAt(data, offset); err != nil {
		t.Fatal(err)
	}

	data[0] ^= 0xFF

	if _, err := f.WriteAt(data, offset); err != nil {
		t.Fatal(err)
	}

	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}

	expectSegmentedCorruption(t, path)
}

func TestSegmentedWALRejectsIncompletePayload(t *testing.T) {
	path := createSegmentedWALForCorruptionTest(t)

	numbers, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	last := segmentPath(path, numbers[len(numbers)-1])

	info, err := os.Stat(last)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Truncate(last, info.Size()-1); err != nil {
		t.Fatal(err)
	}

	expectSegmentedCorruption(t, path)
}

func TestSegmentedWALRejectsIncompleteHeader(t *testing.T) {
	path := createSegmentedWALForCorruptionTest(t)

	numbers, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	last := segmentPath(path, numbers[len(numbers)-1])

	f, err := os.OpenFile(
		last,
		os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	expectSegmentedCorruption(t, path)
}
