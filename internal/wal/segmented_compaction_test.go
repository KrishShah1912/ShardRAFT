package wal

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedWALLogicalCompactionRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	options := SegmentOptions{
		MaxSegmentSize: 256,
	}

	w, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 20; i++ {
		if err := w.AppendEntries([]raft.Entry{{
			Index: i,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  []byte("persisted command"),
		}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 20,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(10, 1); err != nil {
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

	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if state.Snapshot.LastIncludedIndex != 10 {
		t.Fatalf(
			"snapshot index = %d, want 10",
			state.Snapshot.LastIncludedIndex,
		)
	}

	if state.Snapshot.LastIncludedTerm != 1 {
		t.Fatalf(
			"snapshot term = %d, want 1",
			state.Snapshot.LastIncludedTerm,
		)
	}

	if len(state.Log) != 10 {
		t.Fatalf(
			"remaining entries = %d, want 10",
			len(state.Log),
		)
	}

	if state.Log[0].Index != 11 {
		t.Fatalf(
			"first remaining index = %d, want 11",
			state.Log[0].Index,
		)
	}

	if state.HardState.Commit != 20 {
		t.Fatalf(
			"commit index = %d, want 20",
			state.HardState.Commit,
		)
	}
}
