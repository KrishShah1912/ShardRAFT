package raft

import "testing"

func TestCompactPrefixPreservesAbsoluteIndexes(t *testing.T) {
	r, err := NewRaftFromState(
		1, nil, 5, compactedTestState(),
	)
	if err != nil {
		t.Fatal(err)
	}

	r.Advance()

	if err := r.CompactPrefix(5, 3); err != nil {
		t.Fatal(err)
	}

	if r.snapshotIndex != 5 ||
		r.snapshotTerm != 3 {
		t.Fatal("incorrect snapshot boundary")
	}

	if len(r.log) != 1 || r.log[0].Index != 6 {
		t.Fatalf("incorrect remaining log: %+v", r.log)
	}

	if r.lastIndex() != 6 {
		t.Fatalf("expected last index 6, got %d", r.lastIndex())
	}

	if r.termAt(5) != 3 {
		t.Fatal("snapshot boundary term unavailable")
	}

	if rd := r.Ready(); rd.TruncateFrom != 0 ||
		len(rd.Entries) != 0 {
		t.Fatal("compaction produced unexpected WAL changes")
	}
}

func TestCompactPrefixRejectsUnappliedEntries(t *testing.T) {
	r, err := NewRaftFromState(
		1, nil, 5, compactedTestState(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := r.CompactPrefix(5, 3); err == nil {
		t.Fatal("expected unapplied compaction rejection")
	}
}

func TestCompactPrefixRejectsIncorrectTerm(t *testing.T) {
	r, err := NewRaftFromState(
		1, nil, 5, compactedTestState(),
	)
	if err != nil {
		t.Fatal(err)
	}

	r.Advance()

	if err := r.CompactPrefix(5, 2); err == nil {
		t.Fatal("expected term mismatch rejection")
	}
}

func TestCompactPrefixRejectsPendingReady(t *testing.T) {
	r := NewRaft(1, nil, 5)

	for i := 0; i < 5; i++ {
		r.Tick()
	}

	if err := r.CompactPrefix(1, 1); err == nil {
		t.Fatal("expected pending Ready rejection")
	}
}

func TestCompactPrefixAllowsContinuedProposals(t *testing.T) {
	r, err := NewRaftFromState(
		1, nil, 5, compactedTestState(),
	)
	if err != nil {
		t.Fatal(err)
	}

	r.Advance()

	if err := r.CompactPrefix(5, 3); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		r.Tick()
	}

	if r.Status().Role != Leader {
		t.Fatal("expected leader")
	}

	if r.lastIndex() != 7 {
		t.Fatalf(
			"expected no-op at index 7, got %d",
			r.lastIndex(),
		)
	}

	r.Advance()

	if err := r.Propose([]byte("test")); err != nil {
		t.Fatal(err)
	}

	if r.lastIndex() != 8 {
		t.Fatalf(
			"expected proposal at index 8, got %d",
			r.lastIndex(),
		)
	}
}
