package raft

import "testing"

// electDelayedResponseTestLeader creates a leader with
// two followers and a committed-index tracking state.
func electDelayedResponseTestLeader(t *testing.T) *Raft {
	t.Helper()

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
		t.Fatal("expected node 1 to become leader")
	}

	return r
}

// TestDelayedAppendResponseEventuallyRecovers verifies
// that an older successful response can be ignored safely,
// provided a subsequent retry is acknowledged.
func TestDelayedAppendResponseEventuallyRecovers(t *testing.T) {
	r := electDelayedResponseTestLeader(t)

	r.sendAppend(2)
	oldID := r.nextRequestID

	r.sendAppend(2)
	newID := r.nextRequestID

	if oldID == newID {
		t.Fatal("request IDs must differ")
	}

	// The older response arrives after being superseded.
	r.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      r.hard.Term,
		Index:     r.lastIndex(),
		RequestID: oldID,
	})

	if r.matchIndex[2] != 0 {
		t.Fatal("superseded response unexpectedly changed match index")
	}

	// The newer request succeeds.
	r.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      r.hard.Term,
		Index:     r.lastIndex(),
		RequestID: newID,
	})

	if r.matchIndex[2] != r.lastIndex() {
		t.Fatal("latest acknowledgment failed to advance match index")
	}
}

// TestLostAppendAcknowledgmentRetries verifies that
// periodic replication creates a fresh request after
// an acknowledgment is lost.
func TestLostAppendAcknowledgmentRetries(t *testing.T) {
	r := electDelayedResponseTestLeader(t)

	r.sendAppend(2)
	firstID := r.nextRequestID

	// Simulate a lost acknowledgment by not delivering it.
	for i := 0; i < r.heartbeatTimeout; i++ {
		r.Tick()
	}

	if r.nextRequestID <= firstID {
		t.Fatal("leader did not retry AppendEntries")
	}

	if len(r.pendingAppend[2]) > 1 {
		t.Fatal("pending request tracking is unbounded")
	}
}

// TestDelayedAppendResponseCannotRegressProgress verifies
// that a stale response cannot reduce matchIndex.
func TestDelayedAppendResponseCannotRegressProgress(t *testing.T) {
	r := electDelayedResponseTestLeader(t)

	r.sendAppend(2)
	oldID := r.nextRequestID

	r.sendAppend(2)
	newID := r.nextRequestID

	// The latest request succeeds.
	r.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      r.hard.Term,
		Index:     r.lastIndex(),
		RequestID: newID,
	})

	before := r.matchIndex[2]

	// A delayed rejection from the older request arrives.
	r.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      r.hard.Term,
		Index:     0,
		Reject:    true,
		RequestID: oldID,
	})

	if r.matchIndex[2] != before {
		t.Fatal("stale rejection regressed replication progress")
	}
}
