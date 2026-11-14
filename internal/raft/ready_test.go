package raft

import "testing"

func TestReadyExposesPersistentState(t *testing.T) {
	node := NewRaft(1, nil, 5)

	// Trigger a single-node leader election.
	for i := 0; i < 5; i++ {
		node.Tick()
	}

	ready := node.Ready()

	if ready.HardState == nil {
		t.Fatal("expected pending hard state")
	}

	if ready.HardState.Term != 1 {
		t.Fatalf(
			"expected term 1, got %d",
			ready.HardState.Term,
		)
	}

	if ready.HardState.Vote != 1 {
		t.Fatalf(
			"expected vote 1, got %d",
			ready.HardState.Vote,
		)
	}

	if len(ready.Entries) != 1 {
		t.Fatalf(
			"expected one leader no-op, got %d",
			len(ready.Entries),
		)
	}

	if ready.Entries[0].Type != EntryNoop {
		t.Fatal("expected leader no-op entry")
	}

	if len(ready.CommittedEntries) != 1 {
		t.Fatal("expected committed no-op")
	}

	node.Advance()

	next := node.Ready()

	if next.HardState != nil {
		t.Fatal("hard state should be acknowledged")
	}

	if len(next.Entries) != 0 {
		t.Fatal("entries should be acknowledged")
	}

	if len(next.CommittedEntries) != 0 {
		t.Fatal("committed entries should be acknowledged")
	}
}

func TestReadyExposesProposedEntries(t *testing.T) {
	node := NewRaft(1, nil, 5)

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	node.Advance()

	if err := node.Propose([]byte("SET x=10")); err != nil {
		t.Fatal(err)
	}

	ready := node.Ready()

	if len(ready.Entries) != 1 {
		t.Fatalf(
			"expected one proposed entry, got %d",
			len(ready.Entries),
		)
	}

	if ready.Entries[0].Index != 2 {
		t.Fatalf(
			"expected index 2, got %d",
			ready.Entries[0].Index,
		)
	}

	if string(ready.Entries[0].Data) != "SET x=10" {
		t.Fatal("unexpected proposed command")
	}

	if ready.HardState == nil ||
		ready.HardState.Commit != 2 {
		t.Fatal("expected updated commit index")
	}

	if len(ready.CommittedEntries) != 1 {
		t.Fatal("expected committed proposed entry")
	}

	node.Advance()

	next := node.Ready()

	if len(next.Entries) != 0 ||
		len(next.CommittedEntries) != 0 {
		t.Fatal("Advance did not acknowledge pending entries")
	}
}

func TestReadyDetectsLogReplacement(t *testing.T) {
	node := NewRaft(2, []NodeID{1, 3}, 8)

	// Establish an uncommitted log entry.
	node.log = []Entry{
		{Index: 1, Term: 1, Data: []byte("old")},
	}

	node.Advance()

	// Simulate a conflicting entry from a newer leader.
	node.log[0] = Entry{
		Index: 1,
		Term:  2,
		Data:  []byte("new"),
	}

	ready := node.Ready()

	if len(ready.Entries) != 1 {
		t.Fatalf(
			"expected one replacement entry, got %d",
			len(ready.Entries),
		)
	}

	if ready.Entries[0].Term != 2 {
		t.Fatal("replacement term not detected")
	}

	if string(ready.Entries[0].Data) != "new" {
		t.Fatal("replacement data not detected")
	}
}

func TestReadyCopiesEntryData(t *testing.T) {
	node := NewRaft(1, nil, 5)

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	node.Advance()

	if err := node.Propose([]byte("original")); err != nil {
		t.Fatal(err)
	}

	ready := node.Ready()

	if len(ready.Entries) != 1 {
		t.Fatal("expected one entry")
	}

	// Modifying the Ready result must not mutate Raft's log.
	ready.Entries[0].Data[0] = 'X'

	next := node.Ready()

	if string(next.Entries[0].Data) != "original" {
		t.Fatal("Ready exposed mutable internal entry data")
	}
}
