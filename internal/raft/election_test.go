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
