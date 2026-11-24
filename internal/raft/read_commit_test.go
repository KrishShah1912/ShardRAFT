package raft

import (
	"errors"
	"reflect"
	"testing"
)

func TestReadCommitFollower(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 5)

	if r.hasCommittedCurrentTerm() {
		t.Fatal("follower must not satisfy read guard")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitCandidate(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 5)
	r.startElection()

	if r.volatile.Role != Candidate {
		t.Fatal("expected candidate")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitZeroTerm(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.volatile.Role = Leader

	if r.hasCommittedCurrentTerm() {
		t.Fatal("zero term must not satisfy read guard")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitZeroIndex(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})

	if r.hard.Commit != 0 {
		t.Fatal("expected zero commit index")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitUncommittedNoop(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})

	entry, exists := r.entryAt(r.lastIndex())

	if !exists ||
		entry.Type != EntryNoop ||
		entry.Term != r.hard.Term {
		t.Fatal("expected current-term no-op")
	}

	if r.hasCommittedCurrentTerm() {
		t.Fatal("uncommitted no-op must not enable reads")
	}
}

func TestReadCommitPreviousTerm(t *testing.T) {
	r, err := NewRaftFromState(
		1,
		[]NodeID{2, 3},
		5,
		PersistentState{
			HardState: HardState{
				Term:   2,
				Commit: 1,
			},
			Log: []Entry{
				{
					Index: 1,
					Term:  2,
					Type:  EntryNoop,
				},
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	r.startElection()

	err = r.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: r.hard.Term,
	})

	if err != nil {
		t.Fatal(err)
	}

	if r.volatile.Role != Leader {
		t.Fatal("expected leader")
	}

	if r.hard.Term != 3 || r.hard.Commit != 1 {
		t.Fatal("unexpected term or commit index")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitSingleNode(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("single-node leader should commit its no-op")
	}

	index, err := r.readCommitBarrier()

	if err != nil || index != 1 {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitAfterReplication(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})

	if r.hasCommittedCurrentTerm() {
		t.Fatal("read guard enabled before replication")
	}

	r.sendAppend(2)

	var request Message
	found := false

	for _, msg := range r.Ready().Messages {
		if msg.Type == MsgAppend && msg.To == 2 {
			request = msg
			found = true
		}
	}

	if !found {
		t.Fatal("expected AppendEntries request")
	}

	err := r.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        r.id,
		Term:      r.hard.Term,
		RequestID: request.RequestID,
		Index:     r.lastIndex(),
	})

	if err != nil {
		t.Fatal(err)
	}

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("committed no-op should enable read guard")
	}

	index, err := r.readCommitBarrier()

	if err != nil || index != r.hard.Commit || index == 0 {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitUnavailableIndex(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	// Deliberately construct an invalid commit index.
	r.hard.Commit = r.lastIndex() + 1

	if r.hasCommittedCurrentTerm() {
		t.Fatal("unavailable commit index must be rejected")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitLeadershipLoss(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("expected eligible leader")
	}

	r.becomeFollower(r.hard.Term+1, 0)

	if r.hasCommittedCurrentTerm() {
		t.Fatal("leadership loss must invalidate eligibility")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitNewLeadershipTerm(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})

	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("expected eligible leader")
	}

	previousCommit := r.hard.Commit

	r.becomeFollower(r.hard.Term+1, 0)
	r.startElection()

	err := r.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   r.id,
		Term: r.hard.Term,
	})

	if err != nil {
		t.Fatal(err)
	}

	if r.volatile.Role != Leader {
		t.Fatal("expected newly elected leader")
	}

	if r.hard.Commit != previousCommit {
		t.Fatal("unexpected commit advancement")
	}

	if r.hasCommittedCurrentTerm() {
		t.Fatal("previous-term commit must not enable reads")
	}

	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("new current-term commit should enable reads")
	}
}

func TestReadCommitBarrierAdvances(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	first, err := r.readCommitBarrier()

	if err != nil {
		t.Fatal(err)
	}

	if err := r.Propose([]byte("command")); err != nil {
		t.Fatal(err)
	}

	second, err := r.readCommitBarrier()

	if err != nil {
		t.Fatal(err)
	}

	if second != first+1 {
		t.Fatalf("expected advancing barrier: %d -> %d",
			first, second)
	}
}

func TestReadCommitIndependentOfApplication(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	if r.volatile.AppliedIndex >= r.hard.Commit {
		t.Fatal("expected committed but unapplied entry")
	}

	index, err := r.readCommitBarrier()

	if err != nil || index != r.hard.Commit {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}

	// The application barrier is enforced separately.
	if r.volatile.AppliedIndex >= index {
		t.Fatal("guard must not advance application")
	}
}

func TestReadCommitNoStateMutation(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	beforeHard := r.hard
	beforeVolatile := r.volatile
	beforeLog := cloneEntries(r.log)
	beforeReady := r.Ready()
	beforeReadID := r.nextReadID
	beforePending := r.pendingReadCount()

	for i := 0; i < 100; i++ {
		if !r.hasCommittedCurrentTerm() {
			t.Fatal("expected eligible leader")
		}

		if _, err := r.readCommitBarrier(); err != nil {
			t.Fatal(err)
		}
	}

	if r.hard != beforeHard ||
		r.volatile != beforeVolatile ||
		!reflect.DeepEqual(r.log, beforeLog) ||
		!reflect.DeepEqual(r.Ready(), beforeReady) ||
		r.nextReadID != beforeReadID ||
		r.pendingReadCount() != beforePending {
		t.Fatal("read commit guard modified Raft state")
	}
}
