package raft

import "testing"

func TestSingleNodeElection(t *testing.T) {
	node := NewRaft(1, nil, 5)

	if node.Status().Role != Follower {
		t.Fatal("expected initial follower")
	}

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	status := node.Status()

	if status.Role != Leader {
		t.Fatalf("expected leader, got %v", status.Role)
	}

	if status.Term != 1 {
		t.Fatalf("expected term 1, got %d", status.Term)
	}

	if status.LeaderID != 1 {
		t.Fatalf("expected leader ID 1, got %d", status.LeaderID)
	}
}

func TestLeaderAppendsNoopOnElection(t *testing.T) {
	node := NewRaft(1, []NodeID{2, 3}, 5)

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := node.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != Leader {
		t.Fatal("expected node to become leader")
	}

	if len(node.log) != 1 {
		t.Fatalf(
			"expected 1 no-op entry, got %d",
			len(node.log),
		)
	}

	entry := node.log[0]

	if entry.Type != EntryNoop {
		t.Fatal("expected no-op entry")
	}

	if entry.Term != 1 || entry.Index != 1 {
		t.Fatalf(
			"unexpected no-op position: index=%d term=%d",
			entry.Index,
			entry.Term,
		)
	}
}
