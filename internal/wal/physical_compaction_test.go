package wal

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestPhysicalCompactionReducesWALSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	entries := make([]raft.Entry, 100)

	for i := range entries {
		entries[i] = raft.Entry{
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte("sample-command-data"),
		}
	}

	for _, entry := range entries {
		if err := w.AppendEntries([]raft.Entry{entry}); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 90,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(90, 1); err != nil {
		t.Fatal(err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.RewriteCheckpoint(path); err != nil {
		t.Fatal(err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if after.Size() >= before.Size() {
		t.Fatalf(
			"expected smaller WAL: before=%d after=%d",
			before.Size(),
			after.Size(),
		)
	}
}

func TestPhysicalCompactionSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   2,
		Commit: 3,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(2, 1); err != nil {
		t.Fatal(err)
	}

	expected := copyState(w.state)

	if err := w.RewriteCheckpoint(path); err != nil {
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

	actual, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf(
			"recovered state mismatch:\nexpected=%+v\nactual=%+v",
			expected,
			actual,
		)
	}
}

func TestPhysicalCompactionAllowsSubsequentWrites(t *testing.T) {
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
		Term:   1,
		Commit: 2,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(2, 1); err != nil {
		t.Fatal(err)
	}

	if err := w.RewriteCheckpoint(path); err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 4, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   2,
		Commit: 4,
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

	if state.Snapshot.LastIncludedIndex != 2 ||
		state.HardState.Commit != 4 ||
		len(state.Log) != 2 ||
		state.Log[1].Index != 4 {
		t.Fatalf("unexpected recovered state: %+v", state)
	}
}

func TestPhysicalCompactionRejectsMissingBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	err = w.RewriteCheckpoint(path)

	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
}

func TestPhysicalCompactionRejectsWrongPath(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "node.wal")
	wrongPath := filepath.Join(dir, "other.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := os.WriteFile(wrongPath, []byte("other"), 0644); err != nil {
		t.Fatal(err)
	}

	err = w.RewriteCheckpoint(wrongPath)

	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
}
