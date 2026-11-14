package wal

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func openTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "raft.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

func TestWALRecovery(t *testing.T) {
	w, path := openTestWAL(t)

	entries := []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNormal,
			Data: []byte("SET x=10")},
	}

	if err := w.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}

	hs := raft.HardState{Term: 1, Vote: 1, Commit: 1}
	if err := w.SaveHardState(hs); err != nil {
		t.Fatal(err)
	}

	if err := w.Sync(); err != nil {
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

	if !reflect.DeepEqual(state.Log, entries) {
		t.Fatalf("recovered log mismatch: %+v", state.Log)
	}
	if state.HardState != hs {
		t.Fatalf("hard state mismatch: %+v", state.HardState)
	}
}

func TestWALDetectsCorruption(t *testing.T) {
	w, path := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("SET x=10")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.WriteAt([]byte{0xFF}, 12); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	_, err = reopened.Recover()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestWALDetectsTruncation(t *testing.T) {
	w, path := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("SET x=10")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	_, err = reopened.Recover()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestWALReplaceConflictingEntries(t *testing.T) {
	w, path := openTestWAL(t)

	original := []raft.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 2, Data: []byte("old-b")},
		{Index: 3, Term: 2, Data: []byte("old-c")},
	}

	if err := w.AppendEntries(original); err != nil {
		t.Fatal(err)
	}

	replacement := []raft.Entry{
		{Index: 2, Term: 3, Data: []byte("new-b")},
		{Index: 3, Term: 3, Data: []byte("new-c")},
	}

	if err := w.ReplaceEntries(2, replacement); err != nil {
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

	expected := []raft.Entry{
		original[0], replacement[0], replacement[1],
	}
	if !reflect.DeepEqual(state.Log, expected) {
		t.Fatalf("replacement mismatch: %+v", state.Log)
	}
}

func TestWALRejectsCommittedOverwrite(t *testing.T) {
	w, _ := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term: 1, Commit: 2,
	}); err != nil {
		t.Fatal(err)
	}

	err := w.ReplaceEntries(2, []raft.Entry{
		{Index: 2, Term: 2},
	})

	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}

	state, err := w.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if state.Log[1].Term != 1 {
		t.Fatal("committed entry was changed")
	}
}

func TestWALRejectsInvalidIndexes(t *testing.T) {
	w, _ := openTestWAL(t)

	err := w.AppendEntries([]raft.Entry{
		{Index: 2, Term: 1},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected gap rejection, got %v", err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	}); err != nil {
		t.Fatal(err)
	}

	err = w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected duplicate rejection, got %v", err)
	}

	err = w.ReplaceEntries(3, []raft.Entry{
		{Index: 3, Term: 2},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected replacement gap rejection, got %v", err)
	}

	err = w.ReplaceEntries(1, []raft.Entry{
		{Index: 2, Term: 2},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected index mismatch rejection, got %v", err)
	}
}

func TestWALRejectsHardStateRegression(t *testing.T) {
	w, _ := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term: 2, Vote: 1, Commit: 1,
	}); err != nil {
		t.Fatal(err)
	}

	cases := []raft.HardState{
		{Term: 1, Vote: 1, Commit: 1},
		{Term: 2, Vote: 1, Commit: 0},
		{Term: 2, Vote: 2, Commit: 1},
		{Term: 2, Vote: 1, Commit: 3},
	}

	for _, hs := range cases {
		err := w.SaveHardState(hs)
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("expected rejection for %+v, got %v", hs, err)
		}
	}
}

func TestWALRequiresRecoveryBeforeAppending(t *testing.T) {
	w, path := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
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

	err = reopened.AppendEntries([]raft.Entry{
		{Index: 2, Term: 1},
	})
	if !errors.Is(err, ErrNotRecovered) {
		t.Fatalf("expected ErrNotRecovered, got %v", err)
	}

	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}

	if err := reopened.AppendEntries([]raft.Entry{
		{Index: 2, Term: 1},
	}); err != nil {
		t.Fatal(err)
	}

	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Log) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(state.Log))
	}
}

func TestWALRejectsWritesAfterClose(t *testing.T) {
	w, _ := openTestWAL(t)

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	})
	if !errors.Is(err, ErrUnusable) {
		t.Fatalf("expected ErrUnusable, got %v", err)
	}
}
