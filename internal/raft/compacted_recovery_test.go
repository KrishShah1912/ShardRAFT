package raft

import "testing"

func compactedTestState() PersistentState {
	return PersistentState{
		HardState: HardState{
			Term:   3,
			Commit: 5,
		},
		Snapshot: SnapshotMetadata{
			LastIncludedIndex: 3,
			LastIncludedTerm:  2,
		},
		Log: []Entry{
			{Index: 4, Term: 3},
			{Index: 5, Term: 3},
			{Index: 6, Term: 3},
		},
	}
}

func TestRecoverCompactedRaft(t *testing.T) {
	r, err := NewRaftFromState(
		1, nil, 5, compactedTestState(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if r.snapshotIndex != 3 || r.snapshotTerm != 2 {
		t.Fatal("snapshot boundary not restored")
	}

	if r.lastIndex() != 6 {
		t.Fatalf("expected last index 6, got %d", r.lastIndex())
	}

	if r.termAt(3) != 2 || r.termAt(4) != 3 {
		t.Fatal("incorrect terms after recovery")
	}

	if r.volatile.AppliedIndex != 3 {
		t.Fatalf(
			"expected applied index 3, got %d",
			r.volatile.AppliedIndex,
		)
	}

	ready := r.Ready()
	if len(ready.CommittedEntries) != 2 {
		t.Fatalf(
			"expected 2 entries for replay, got %d",
			len(ready.CommittedEntries),
		)
	}

	if ready.CommittedEntries[0].Index != 4 ||
		ready.CommittedEntries[1].Index != 5 {
		t.Fatal("incorrect committed entries for replay")
	}
}

func TestRecoverCompactedRaftRejectsInvalidBoundary(t *testing.T) {
	state := compactedTestState()
	state.Snapshot.LastIncludedTerm = 0

	if _, err := NewRaftFromState(
		1, nil, 5, state,
	); err == nil {
		t.Fatal("expected invalid snapshot boundary rejection")
	}
}

func TestRecoverCompactedRaftRejectsMissingEntries(t *testing.T) {
	state := compactedTestState()
	state.Log[0].Index = 5

	if _, err := NewRaftFromState(
		1, nil, 5, state,
	); err == nil {
		t.Fatal("expected log gap rejection")
	}
}

func TestRecoverCompactedRaftElection(t *testing.T) {
	r, err := NewRaftFromState(
		1, nil, 5, compactedTestState(),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Replay the committed suffix before starting elections.
	r.Advance()

	for i := 0; i < 5; i++ {
		r.Tick()
	}

	if r.Status().Role != Leader {
		t.Fatal("expected recovered node to become leader")
	}

	if r.lastIndex() != 7 {
		t.Fatalf(
			"expected new no-op at index 7, got %d",
			r.lastIndex(),
		)
	}

	if r.termAt(7) != 4 {
		t.Fatalf(
			"expected new term 4, got %d",
			r.termAt(7),
		)
	}
}
