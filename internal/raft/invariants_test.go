package raft

import "testing"

func TestCheckInvariantsValidNode(t *testing.T) {
	r := NewRaft(1, nil, 5)

	if err := r.CheckInvariants(); err != nil {
		t.Fatalf("valid node failed invariants: %v", err)
	}
}

func TestCheckInvariantsAppliedExceedsCommit(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.volatile.AppliedIndex = 1

	if err := r.CheckInvariants(); err == nil {
		t.Fatal("expected applied-index invariant failure")
	}
}

func TestCheckInvariantsCommitExceedsLog(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.hard.Commit = 2
	r.volatile.CommitIndex = 2

	if err := r.CheckInvariants(); err == nil {
		t.Fatal("expected commit-index invariant failure")
	}
}

func TestCheckInvariantsInvalidLogIndex(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.hard.Term = 1

	r.log = []Entry{
		{Index: 2, Term: 1, Type: EntryNormal},
	}

	if err := r.CheckInvariants(); err == nil {
		t.Fatal("expected log-index invariant failure")
	}
}

func TestCheckInvariantsInvalidLogTerm(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.hard.Term = 1

	r.log = []Entry{
		{Index: 1, Term: 2, Type: EntryNormal},
	}

	if err := r.CheckInvariants(); err == nil {
		t.Fatal("expected log-term invariant failure")
	}
}

func TestCheckInvariantsVolatileCommitMismatch(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.hard.Term = 1

	r.log = []Entry{
		{Index: 1, Term: 1, Type: EntryNormal},
	}

	r.hard.Commit = 1
	r.volatile.CommitIndex = 0

	if err := r.CheckInvariants(); err == nil {
		t.Fatal("expected volatile commit mismatch failure")
	}
}

func TestCheckInvariantsValidCommittedEntry(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.hard.Term = 1

	r.log = []Entry{
		{Index: 1, Term: 1, Type: EntryNormal},
	}

	r.hard.Commit = 1
	r.volatile.CommitIndex = 1
	r.volatile.AppliedIndex = 1

	if err := r.CheckInvariants(); err != nil {
		t.Fatalf("valid committed entry failed: %v", err)
	}
}
