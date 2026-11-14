package wal

import (
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestWALTruncateSuffixRecovery(t *testing.T) {
	w, path := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.TruncateSuffix(2); err != nil {
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

	if len(state.Log) != 1 {
		t.Fatalf(
			"expected 1 recovered entry, got %d",
			len(state.Log),
		)
	}

	if state.Log[0].Index != 1 {
		t.Fatal("incorrect recovered log")
	}
}

func TestWALRejectsCommittedTruncation(t *testing.T) {
	w, _ := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 2,
	}); err != nil {
		t.Fatal(err)
	}

	err := w.TruncateSuffix(2)

	if !errors.Is(err, ErrInvalid) {
		t.Fatalf(
			"expected ErrInvalid, got %v",
			err,
		)
	}
}

func TestWALTruncateAndAppend(t *testing.T) {
	w, path := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 2, Term: 3},
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

	if len(state.Log) != 2 {
		t.Fatalf(
			"expected 2 entries, got %d",
			len(state.Log),
		)
	}

	if state.Log[1].Term != 3 {
		t.Fatal("replacement term was not recovered")
	}
}
