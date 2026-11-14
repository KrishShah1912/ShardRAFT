package wal

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestWALCompactionSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compacted.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 2},
	}

	if err := w.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   2,
		Commit: 4,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(3, 2); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if state.Snapshot.LastIncludedIndex != 3 ||
		state.Snapshot.LastIncludedTerm != 2 {
		t.Fatalf(
			"incorrect snapshot metadata: %+v",
			state.Snapshot,
		)
	}

	if len(state.Log) != 2 {
		t.Fatalf(
			"expected 2 remaining entries, got %d",
			len(state.Log),
		)
	}

	if state.Log[0].Index != 4 ||
		state.Log[1].Index != 5 {
		t.Fatalf(
			"incorrect remaining indexes: %+v",
			state.Log,
		)
	}
}

func TestWALRejectsCompactingUncommittedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(2, 1); err == nil {
		t.Fatal("expected uncommitted compaction rejection")
	}
}

func TestWALAppendAfterCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   2,
		Commit: 2,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(2, 1); err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 4, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Log) != 2 ||
		state.Log[0].Index != 3 ||
		state.Log[1].Index != 4 {
		t.Fatalf(
			"unexpected log after recovery: %+v",
			state.Log,
		)
	}

	if state.Snapshot.LastIncludedIndex != 2 {
		t.Fatalf(
			"unexpected snapshot boundary: %+v",
			state.Snapshot,
		)
	}
}

func TestWALRejectsReplacingCompactedPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 2,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(2, 1); err != nil {
		t.Fatal(err)
	}

	err = w.ReplaceEntries(2, []raft.Entry{
		{Index: 2, Term: 2},
	})

	if err == nil {
		t.Fatal("expected compacted prefix replacement rejection")
	}
}

func TestWALRejectsTruncatingCompactedPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 2,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(2, 1); err != nil {
		t.Fatal(err)
	}

	if err := w.TruncateSuffix(2); err == nil {
		t.Fatal("expected compacted prefix truncation rejection")
	}
}
