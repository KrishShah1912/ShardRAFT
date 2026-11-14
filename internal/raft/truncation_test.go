package raft

import "testing"

func TestReadyDetectsPureTruncation(t *testing.T) {
	node := NewRaft(2, []NodeID{1, 3}, 8)

	node.log = []Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
	}

	node.Advance()

	node.log = node.log[:1]

	ready := node.Ready()

	if ready.TruncateFrom != 2 {
		t.Fatalf(
			"expected truncation from 2, got %d",
			ready.TruncateFrom,
		)
	}

	if len(ready.Entries) != 0 {
		t.Fatal("pure truncation should not append entries")
	}
}

func TestReadyDetectsReplacementTruncation(t *testing.T) {
	node := NewRaft(2, []NodeID{1, 3}, 8)

	node.log = []Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
	}

	node.Advance()

	node.log = []Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 3},
	}

	ready := node.Ready()

	if ready.TruncateFrom != 2 {
		t.Fatalf(
			"expected truncation from 2, got %d",
			ready.TruncateFrom,
		)
	}

	if len(ready.Entries) != 1 ||
		ready.Entries[0].Term != 3 {
		t.Fatal("expected replacement entry at index 2")
	}
}

func TestReadyDetectsSimpleAppend(t *testing.T) {
	node := NewRaft(2, []NodeID{1, 3}, 8)

	node.log = []Entry{
		{Index: 1, Term: 1},
	}

	node.Advance()

	node.log = append(node.log, Entry{
		Index: 2,
		Term:  2,
	})

	ready := node.Ready()

	if ready.TruncateFrom != 0 {
		t.Fatalf(
			"unexpected truncation from %d",
			ready.TruncateFrom,
		)
	}

	if len(ready.Entries) != 1 ||
		ready.Entries[0].Index != 2 {
		t.Fatal("expected appended entry at index 2")
	}
}
