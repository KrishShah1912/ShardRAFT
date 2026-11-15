package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedWALFailedRecoveryIsUnusable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("valid"),
	}}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	segment := segmentPath(path, 1)

	f, err := os.OpenFile(segment, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := reopened.Recover(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}

	if err := reopened.AppendEntries([]raft.Entry{{
		Index: 2,
		Term:  1,
		Type:  raft.EntryNormal,
	}}); !errors.Is(err, ErrUnusable) {
		t.Fatalf("expected ErrUnusable after recovery failure, got %v", err)
	}

	if _, err := reopened.Recover(); !errors.Is(err, ErrUnusable) {
		t.Fatalf("expected repeated recovery to fail, got %v", err)
	}
}
