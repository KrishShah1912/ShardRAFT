package wal

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedWALRecoveryAcrossSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(
		path, SegmentOptions{MaxSegmentSize: 256},
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 20; i++ {
		if err := w.AppendEntries([]raft.Entry{{
			Index: i,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  []byte("persisted payload"),
		}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.SaveHardState(raft.HardState{
		Term: 1, Commit: 20,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(
		path, SegmentOptions{MaxSegmentSize: 256},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if err := reopened.AppendEntries([]raft.Entry{{
		Index: 21, Term: 1,
	}}); !errors.Is(err, ErrNotRecovered) {
		t.Fatalf("expected ErrNotRecovered, got %v", err)
	}

	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Log) != 20 {
		t.Fatalf("recovered %d entries, want 20", len(state.Log))
	}
	if state.HardState.Commit != 20 {
		t.Fatalf("commit = %d, want 20", state.HardState.Commit)
	}

	if err := reopened.AppendEntries([]raft.Entry{{
		Index: 21,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("after restart"),
	}}); err != nil {
		t.Fatal(err)
	}
}
