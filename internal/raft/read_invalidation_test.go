package raft

import (
	"errors"
	"reflect"
	"testing"
)

func requireInvalidatedReads(t *testing.T, r *Raft) {
	t.Helper()

	if r.pendingReadCount() != 0 {
		t.Fatal("pending reads were not invalidated")
	}

	if r.completedReadCount() != 0 {
		t.Fatal("completed reads were not invalidated")
	}

	for _, msg := range r.messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID != 0 {
			t.Fatal("obsolete read probe remains queued")
		}
	}
}

func TestReadInvalidationPendingRequests(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	for i := 0; i < 10; i++ {
		startAutomaticRead(t, r, "pending")
	}

	r.becomeFollower(r.hard.Term+1, 0)

	requireInvalidatedReads(t, r)
}

func TestReadInvalidationCompletedRequests(t *testing.T) {
	r := newReadAPILeader(t, nil)

	for i := 0; i < 10; i++ {
		if err := r.ReadIndex([]byte("completed")); err != nil {
			t.Fatal(err)
		}
	}

	if r.completedReadCount() != 10 {
		t.Fatal("expected completed reads")
	}

	r.becomeFollower(r.hard.Term+1, 0)

	requireInvalidatedReads(t, r)
}

func TestReadInvalidationMixedRequests(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	first := startAutomaticRead(t, r, "completed")
	startAutomaticRead(t, r, "pending")

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, first, 2),
	)

	if r.pendingReadCount() != 1 ||
		r.completedReadCount() != 1 {
		t.Fatal("expected mixed read state")
	}

	r.becomeFollower(r.hard.Term+1, 0)

	requireInvalidatedReads(t, r)
}

func TestReadInvalidationSameTermStepDown(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	startAutomaticRead(t, r, "same-term")

	term := r.hard.Term

	r.becomeFollower(term, 2)

	requireInvalidatedReads(t, r)

	if r.hard.Term != term ||
		r.volatile.Role != Follower {
		t.Fatal("incorrect leadership transition")
	}
}

func TestReadInvalidationHigherTermResponse(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "higher-term")

	reply := readHeartbeatAck(r, request, 2)
	reply.Term++

	stepReadHeartbeatAck(t, r, reply)

	requireInvalidatedReads(t, r)

	if r.volatile.Role != Follower ||
		r.hard.Term != reply.Term {
		t.Fatal("higher-term transition failed")
	}
}

func TestReadInvalidationHigherTermProbe(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	startAutomaticRead(t, r, "higher-term-probe")

	msg := Message{
		Type:      MsgHeartbeat,
		From:      2,
		To:        r.id,
		Term:      r.hard.Term + 1,
		RequestID: 101,
	}

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	requireInvalidatedReads(t, r)

	if r.volatile.Role != Follower ||
		r.hard.Term != msg.Term {
		t.Fatal("higher-term probe did not step down leader")
	}

	found := false

	for _, reply := range r.messages {
		if reply.Type == MsgHeartbeatResp &&
			reply.RequestID == msg.RequestID &&
			!reply.Reject {
			found = true
		}
	}

	if !found {
		t.Fatal("valid follower response was lost")
	}
}

func TestReadInvalidationDelayedResponse(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "delayed")
	reply := readHeartbeatAck(r, request, 2)

	r.becomeFollower(r.hard.Term+1, 0)
	r.Advance()

	stepReadHeartbeatAck(t, r, reply)

	requireInvalidatedReads(t, r)
}

func TestReadInvalidationRequestIDPreserved(t *testing.T) {
	r := newReadAPILeader(t, nil)

	if err := r.ReadIndex([]byte("first")); err != nil {
		t.Fatal(err)
	}

	oldID := r.nextReadID

	r.becomeFollower(r.hard.Term+1, 0)
	r.Advance()

	requireInvalidatedReads(t, r)

	if r.nextReadID != oldID {
		t.Fatal("request ID sequence was reset")
	}

	r.startElection()
	r.Advance()

	if r.volatile.Role != Leader {
		t.Fatal("single-node reelection failed")
	}

	if err := r.ReadIndex([]byte("second")); err != nil {
		t.Fatal(err)
	}

	if r.nextReadID != oldID+1 {
		t.Fatal("request ID was reused")
	}

	if r.completedReadCount() != 1 ||
		string(r.completedReads[0].Context) != "second" {
		t.Fatal("obsolete confirmation survived reelection")
	}
}

func TestReadInvalidationNewLeadershipTerm(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	old := startAutomaticRead(t, r, "old")
	oldReply := readHeartbeatAck(r, old, 2)

	r.becomeFollower(r.hard.Term+1, 0)
	r.Advance()

	r.startElection()

	if err := r.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   r.id,
		Term: r.hard.Term,
	}); err != nil {
		t.Fatal(err)
	}

	if r.volatile.Role != Leader {
		t.Fatal("reelection failed")
	}

	err := r.ReadIndex([]byte("too-early"))

	if !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected error: %v", err)
	}

	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()
	r.Advance()

	current := startAutomaticRead(t, r, "new")

	if current.id <= old.id ||
		current.term <= old.term {
		t.Fatal("new leadership reused obsolete identity")
	}

	stepReadHeartbeatAck(t, r, oldReply)

	if r.completedReadCount() != 0 ||
		r.pendingReads[current.id] != current {
		t.Fatal("obsolete response influenced new request")
	}

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, current, 2),
	)

	if r.completedReadCount() != 1 ||
		r.pendingReadCount() != 0 {
		t.Fatal("new leadership read did not complete")
	}

	if string(r.completedReads[0].Context) != "new" {
		t.Fatal("incorrect confirmation")
	}
}

func TestReadInvalidationMalformedResponsePreservesReads(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	first := startAutomaticRead(t, r, "completed")
	second := startAutomaticRead(t, r, "pending")

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, first, 2),
	)

	reply := readHeartbeatAck(r, second, 2)
	reply.Term++
	reply.Index = 1

	stepReadHeartbeatAck(t, r, reply)

	if r.volatile.Role != Leader ||
		r.pendingReadCount() != 1 ||
		r.completedReadCount() != 1 {
		t.Fatal("malformed response invalidated valid reads")
	}
}

func TestReadInvalidationPreservesReplication(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	startAutomaticRead(t, r, "pending")

	r.sendHeartbeats()

	var expected []Message

	for _, msg := range r.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID != 0 {
			continue
		}

		expected = append(expected, msg)
	}

	if len(expected) == 0 {
		t.Fatal("expected ordinary replication messages")
	}

	r.invalidateReads()

	requireInvalidatedReads(t, r)

	if !reflect.DeepEqual(expected, r.Ready().Messages) {
		t.Fatal("ordinary replication messages were modified")
	}
}

func TestReadInvalidationIdempotent(t *testing.T) {
	r := newReadAPILeader(t, nil)

	if err := r.ReadIndex([]byte("completed")); err != nil {
		t.Fatal(err)
	}

	id := r.nextReadID

	for i := 0; i < 100; i++ {
		r.invalidateReads()
	}

	requireInvalidatedReads(t, r)

	if r.nextReadID != id {
		t.Fatal("invalidation reset request identity")
	}
}

func TestReadInvalidationNoDurableChanges(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	startAutomaticRead(t, r, "pending")

	beforeHard := r.hard
	beforeVolatile := r.volatile
	beforeLog := cloneEntries(r.log)
	beforeSnapshot := r.SnapshotMetadata()
	beforeAppendID := r.nextRequestID
	beforeReadID := r.nextReadID

	r.invalidateReads()

	requireInvalidatedReads(t, r)

	if r.hard != beforeHard ||
		r.volatile != beforeVolatile ||
		!reflect.DeepEqual(beforeLog, r.log) ||
		r.SnapshotMetadata() != beforeSnapshot ||
		r.nextRequestID != beforeAppendID ||
		r.nextReadID != beforeReadID {
		t.Fatal("read invalidation changed consensus state")
	}

	rd := r.Ready()

	if rd.HardState != nil ||
		rd.TruncateFrom != 0 ||
		len(rd.Entries) != 0 ||
		len(rd.CommittedEntries) != 0 {
		t.Fatal("invalidation generated durable work")
	}
}
