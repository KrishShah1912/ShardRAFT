package wal

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func compactionTestState() raft.PersistentState {
	return raft.PersistentState{
		HardState: raft.HardState{
			Term:   2,
			Commit: 3,
		},
		Log: []raft.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 2},
			{Index: 4, Term: 2},
		},
	}
}

func TestValidateCompaction(t *testing.T) {
	state := compactionTestState()

	if err := ValidateCompaction(state, 3, 2); err != nil {
		t.Fatalf("expected valid compaction: %v", err)
	}
}

func TestRejectUncommittedCompaction(t *testing.T) {
	state := compactionTestState()

	if err := ValidateCompaction(state, 4, 2); err == nil {
		t.Fatal("expected uncommitted compaction rejection")
	}
}

func TestRejectIncorrectCompactionTerm(t *testing.T) {
	state := compactionTestState()

	if err := ValidateCompaction(state, 3, 1); err == nil {
		t.Fatal("expected term mismatch rejection")
	}
}

func TestRejectZeroCompactionIndex(t *testing.T) {
	state := compactionTestState()

	if err := ValidateCompaction(state, 0, 0); err == nil {
		t.Fatal("expected zero-index rejection")
	}
}

func TestValidateExistingSnapshotBoundary(t *testing.T) {
	state := raft.PersistentState{
		HardState: raft.HardState{
			Term:   3,
			Commit: 5,
		},
		Snapshot: raft.SnapshotMetadata{
			LastIncludedIndex: 3,
			LastIncludedTerm:  2,
		},
		Log: []raft.Entry{
			{Index: 4, Term: 3},
			{Index: 5, Term: 3},
		},
	}

	if err := ValidateCompaction(state, 3, 2); err != nil {
		t.Fatal(err)
	}

	if err := ValidateCompaction(state, 5, 3); err != nil {
		t.Fatal(err)
	}
}

func TestRejectCompactionRegression(t *testing.T) {
	state := raft.PersistentState{
		HardState: raft.HardState{
			Term:   3,
			Commit: 5,
		},
		Snapshot: raft.SnapshotMetadata{
			LastIncludedIndex: 3,
			LastIncludedTerm:  2,
		},
		Log: []raft.Entry{
			{Index: 4, Term: 3},
			{Index: 5, Term: 3},
		},
	}

	if err := ValidateCompaction(state, 2, 2); err == nil {
		t.Fatal("expected compaction regression rejection")
	}
}
