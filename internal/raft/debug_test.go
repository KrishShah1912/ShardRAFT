package raft

import "testing"

func TestDebugStateReturnsIndependentReplicationMaps(t *testing.T) {
	r := NewRaft(1, []NodeID{2}, 1)

	// Start an election.
	r.Tick()

	if r.Status().Role != Candidate {
		t.Fatalf(
			"expected candidate before peer vote, got %d",
			r.Status().Role,
		)
	}

	// Simulate peer 2 granting its vote so node 1
	// legitimately reaches quorum and becomes leader.
	err := r.Step(Message{
		Type:   MsgVoteResp,
		From:   2,
		To:     1,
		Term:   r.Status().Term,
		Reject: false,
	})
	if err != nil {
		t.Fatalf("step vote response: %v", err)
	}

	state := r.DebugState()

	if state.ID != 1 {
		t.Fatalf("expected node 1, got %d", state.ID)
	}

	if state.Role != Leader {
		t.Fatalf("expected leader, got %d", state.Role)
	}

	if len(state.Peers) != 1 {
		t.Fatalf(
			"expected one peer, got %d",
			len(state.Peers),
		)
	}

	state.Peers[0].PendingAppend[999] = 999

	if _, exists := r.pendingAppend[2][999]; exists {
		t.Fatal(
			"debug state exposed internal pending append map",
		)
	}
}

func TestDebugLogCopiesEntryData(t *testing.T) {
	r := NewRaft(1, nil, 1)

	// A single-node cluster reaches quorum immediately.
	r.Tick()

	if r.Status().Role != Leader {
		t.Fatalf(
			"expected single-node leader, got %d",
			r.Status().Role,
		)
	}

	if err := r.Propose([]byte("hello")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	debug := r.DebugLog()

	if len(debug.Entries) < 2 {
		t.Fatalf(
			"expected no-op and proposal, got %d entries",
			len(debug.Entries),
		)
	}

	last := &debug.Entries[len(debug.Entries)-1]

	if len(last.Data) == 0 {
		t.Fatal("expected proposal payload")
	}

	last.Data[0] = 'X'

	internal, ok := r.entryAt(last.Index)
	if !ok {
		t.Fatal("internal entry missing")
	}

	if string(internal.Data) != "hello" {
		t.Fatal(
			"debug log mutation leaked into Raft",
		)
	}
}
