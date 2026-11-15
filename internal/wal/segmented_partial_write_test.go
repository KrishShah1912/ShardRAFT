package wal

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedWALPartialWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}

	first := raft.Entry{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("committed record"),
	}

	if err := w.AppendEntries([]raft.Entry{first}); err != nil {
		t.Fatal(err)
	}

	before := copyState(w.state)

	injected := errors.New("simulated disk failure")

	w.afterHeaderWrite = func() error {
		return injected
	}

	err = w.AppendEntries([]raft.Entry{{
		Index: 2,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("incomplete record"),
	}})

	if !errors.Is(err, injected) {
		t.Fatalf("expected injected failure, got %v", err)
	}

	if !w.unusable {
		t.Fatal("WAL must become unusable")
	}

	if len(w.state.Log) != len(before.Log) {
		t.Fatal("failed write advanced in-memory log")
	}

	if err := w.AppendEntries(nil); !errors.Is(err, ErrUnusable) {
		t.Fatalf("expected ErrUnusable, got %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	// Strict recovery rejects the incomplete record.
	if _, err := reopened.Recover(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}
