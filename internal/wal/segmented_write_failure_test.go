package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedWALRejectsWritesAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries(nil); !errors.Is(err, ErrUnusable) {
		t.Fatalf("empty append after close: %v", err)
	}

	if err := w.Sync(); !errors.Is(err, ErrUnusable) {
		t.Fatalf("sync after close: %v", err)
	}
}

func TestSegmentedWALRotationFailureMakesWALUnusable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(
		path,
		SegmentOptions{MaxSegmentSize: 256},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.AppendEntries([]raft.Entry{{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("first entry"),
	}}); err != nil {
		t.Fatal(err)
	}

	// Pre-create the next segment so O_EXCL rotation fails.
	next := segmentPath(path, 2)

	if err := os.WriteFile(next, []byte("occupied"), 0644); err != nil {
		t.Fatal(err)
	}

	// Force the next append to require rotation.
	w.options.MaxSegmentSize = w.size + 1

	err = w.AppendEntries([]raft.Entry{{
		Index: 2,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("second entry"),
	}})
	if err == nil {
		t.Fatal("expected rotation failure")
	}

	if !w.unusable {
		t.Fatal("WAL should be unusable after rotation failure")
	}

	if err := w.AppendEntries(nil); !errors.Is(err, ErrUnusable) {
		t.Fatalf("expected ErrUnusable, got %v", err)
	}

	if err := w.Sync(); !errors.Is(err, ErrUnusable) {
		t.Fatalf("expected ErrUnusable from Sync, got %v", err)
	}
}
