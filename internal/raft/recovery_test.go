package raft

import "testing"

func TestNewRaftFromState(t *testing.T) {
	state := PersistentState{
		HardState: HardState{
			Term:   4,
			Vote:   2,
			Commit: 2,
		},
		Log: []Entry{
			{Index: 1, Term: 2, Type: EntryNormal, Data: []byte("a")},
			{Index: 2, Term: 4, Type: EntryNormal, Data: []byte("b")},
		},
	}

	node, err := NewRaftFromState(
		2,
		[]NodeID{1, 3},
		8,
		state,
	)
	if err != nil {
		t.Fatal(err)
	}

	if node.volatile.Role != Follower {
		t.Fatal("recovered node must start as follower")
	}

	if node.hard != state.HardState {
		t.Fatal("hard state not restored")
	}

	if len(node.log) != 2 {
		t.Fatal("log not restored")
	}

	ready := node.Ready()

	if ready.HardState != nil {
		t.Fatal("recovered hard state should already be stable")
	}

	if len(ready.Entries) != 0 {
		t.Fatal("recovered log should already be stable")
	}

	if len(ready.CommittedEntries) != 2 {
		t.Fatal("expected committed entries for replay")
	}
}

func TestNewRaftFromStateRejectsInvalidCommit(t *testing.T) {
	state := PersistentState{
		HardState: HardState{
			Term:   2,
			Commit: 3,
		},
		Log: []Entry{
			{Index: 1, Term: 1},
		},
	}

	_, err := NewRaftFromState(1, nil, 5, state)

	if err == nil {
		t.Fatal("expected invalid commit error")
	}
}

func TestNewRaftFromStateCopiesLog(t *testing.T) {
	state := PersistentState{
		HardState: HardState{Term: 1},
		Log: []Entry{
			{
				Index: 1,
				Term:  1,
				Data:  []byte("original"),
			},
		},
	}

	node, err := NewRaftFromState(1, nil, 5, state)
	if err != nil {
		t.Fatal(err)
	}

	state.Log[0].Data[0] = 'X'

	if string(node.log[0].Data) != "original" {
		t.Fatal("recovered log shares mutable data")
	}
}
