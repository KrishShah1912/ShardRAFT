package raft

import "testing"

// TestPendingAppendRequestsBounded verifies that repeated
// replication attempts cannot grow the pending request map
// indefinitely when a follower is unreachable.
func TestPendingAppendRequestsBounded(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 5)

	// Elect node 1 with a majority vote.
	for i := 0; i < 5; i++ {
		r.Tick()
	}

	r.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: r.hard.Term,
	})

	if r.volatile.Role != Leader {
		t.Fatal("node did not become leader")
	}

	// Simulate repeated sends without delivering responses.
	for i := 0; i < 1000; i++ {
		r.sendAppend(2)

		if got := len(r.pendingAppend[2]); got > 1 {
			t.Fatalf(
				"pending requests grew to %d at iteration %d",
				got,
				i,
			)
		}
	}
}

// TestStaleAppendResponseIgnored verifies that an old
// response cannot update replication progress after a
// newer request supersedes it.
func TestStaleAppendResponseIgnored(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 5)

	for i := 0; i < 5; i++ {
		r.Tick()
	}

	r.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: r.hard.Term,
	})

	if r.volatile.Role != Leader {
		t.Fatal("node did not become leader")
	}

	r.sendAppend(2)
	oldRequestID := r.nextRequestID

	r.sendAppend(2)
	newRequestID := r.nextRequestID

	if oldRequestID == newRequestID {
		t.Fatal("request IDs did not advance")
	}

	before := r.matchIndex[2]

	r.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      r.hard.Term,
		Index:     r.lastIndex(),
		RequestID: oldRequestID,
	})

	if r.matchIndex[2] != before {
		t.Fatal("stale response incorrectly updated match index")
	}

	if _, exists := r.pendingAppend[2][newRequestID]; !exists {
		t.Fatal("newer request was incorrectly removed")
	}
}
