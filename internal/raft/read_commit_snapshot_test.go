package raft

import (
	"errors"
	"reflect"
	"testing"
)

func TestReadCommitSnapshotAtCommitBoundary(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	index, err := r.readCommitBarrier()
	if err != nil {
		t.Fatal(err)
	}

	term := r.hard.Term

	r.Advance()

	if err := r.CompactPrefix(index, term); err != nil {
		t.Fatal(err)
	}

	if len(r.log) != 0 {
		t.Fatal("expected fully compacted log")
	}

	if r.snapshotIndex != index ||
		r.snapshotTerm != term {
		t.Fatal("incorrect snapshot boundary")
	}

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("compaction invalidated read eligibility")
	}

	barrier, err := r.readCommitBarrier()

	if err != nil || barrier != index {
		t.Fatalf("unexpected barrier: %d, %v", barrier, err)
	}
}

func TestReadCommitSnapshotRetainedSuffix(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	for i := 0; i < 5; i++ {
		if err := r.Propose([]byte("command")); err != nil {
			t.Fatal(err)
		}
	}

	r.Advance()

	commit := r.hard.Commit

	if err := r.CompactPrefix(3, r.hard.Term); err != nil {
		t.Fatal(err)
	}

	if len(r.log) != 3 {
		t.Fatalf("unexpected retained log length: %d", len(r.log))
	}

	index, err := r.readCommitBarrier()

	if err != nil || index != commit {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitSnapshotPreviousTerm(t *testing.T) {
	r, err := NewRaftFromState(
		1,
		[]NodeID{2, 3},
		5,
		PersistentState{
			HardState: HardState{
				Term:   3,
				Commit: 5,
			},
			Snapshot: SnapshotMetadata{
				LastIncludedIndex: 5,
				LastIncludedTerm:  3,
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	r.startElection()

	if err := r.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: r.hard.Term,
	}); err != nil {
		t.Fatal(err)
	}

	if r.volatile.Role != Leader {
		t.Fatal("expected leader")
	}

	if r.hard.Term != 4 ||
		r.hard.Commit != 5 {
		t.Fatal("unexpected term or commit")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitSnapshotRecoveredFollower(t *testing.T) {
	r, err := NewRaftFromState(
		1,
		nil,
		5,
		PersistentState{
			HardState: HardState{
				Term:   3,
				Commit: 5,
			},
			Snapshot: SnapshotMetadata{
				LastIncludedIndex: 5,
				LastIncludedTerm:  3,
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if r.hasCommittedCurrentTerm() {
		t.Fatal("recovered follower must not enable reads")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitSnapshotRecoveredSingleNode(t *testing.T) {
	r, err := NewRaftFromState(
		1,
		nil,
		5,
		PersistentState{
			HardState: HardState{
				Term:   3,
				Commit: 5,
			},
			Snapshot: SnapshotMetadata{
				LastIncludedIndex: 5,
				LastIncludedTerm:  3,
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	r.startElection()

	if r.volatile.Role != Leader {
		t.Fatal("expected leader")
	}

	if r.hard.Term != 4 {
		t.Fatal("expected new leadership term")
	}

	index, err := r.readCommitBarrier()

	if err != nil || index != 6 {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}

	if r.snapshotIndex != 5 {
		t.Fatal("snapshot boundary changed unexpectedly")
	}
}

func TestReadCommitSnapshotNewTermReplication(t *testing.T) {
	r, err := NewRaftFromState(
		1,
		[]NodeID{2, 3},
		5,
		PersistentState{
			HardState: HardState{
				Term:   3,
				Commit: 5,
			},
			Snapshot: SnapshotMetadata{
				LastIncludedIndex: 5,
				LastIncludedTerm:  3,
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	r.startElection()

	if err := r.Step(Message{
		Type: MsgVoteResp,
		From: 2,
		To:   1,
		Term: r.hard.Term,
	}); err != nil {
		t.Fatal(err)
	}

	if r.volatile.Role != Leader {
		t.Fatal("expected leader")
	}

	if r.hasCommittedCurrentTerm() {
		t.Fatal("previous-term snapshot enabled reads")
	}

	var appendRequest Message
	found := false

	for _, msg := range r.Ready().Messages {
		if msg.Type == MsgAppend && msg.To == 2 {
			appendRequest = msg
			found = true
		}
	}

	if !found {
		t.Fatal("expected AppendEntries request")
	}

	if err := r.Step(Message{
		Type:      MsgAppendResp,
		From:      2,
		To:        1,
		Term:      r.hard.Term,
		RequestID: appendRequest.RequestID,
		Index:     r.lastIndex(),
	}); err != nil {
		t.Fatal(err)
	}

	index, err := r.readCommitBarrier()

	if err != nil || index != 6 {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitSnapshotUnavailableCompactedIndex(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	for i := 0; i < 4; i++ {
		if err := r.Propose([]byte("command")); err != nil {
			t.Fatal(err)
		}
	}

	r.Advance()

	if err := r.CompactPrefix(5, r.hard.Term); err != nil {
		t.Fatal(err)
	}

	// Deliberately construct an invalid commit index
	// preceding the snapshot boundary.
	r.hard.Commit = 4

	if r.hasCommittedCurrentTerm() {
		t.Fatal("unavailable compacted index enabled reads")
	}

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitSnapshotRepeatedCompaction(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()

	for i := 0; i < 20; i++ {
		if err := r.Propose([]byte("command")); err != nil {
			t.Fatal(err)
		}

		r.Advance()

		index := r.hard.Commit

		if err := r.CompactPrefix(index, r.hard.Term); err != nil {
			t.Fatal(err)
		}

		barrier, err := r.readCommitBarrier()

		if err != nil || barrier != index {
			t.Fatalf(
				"round %d: unexpected barrier %d, %v",
				i, barrier, err,
			)
		}

		if len(r.log) != 0 {
			t.Fatal("expected fully compacted log")
		}
	}
}

func TestReadCommitSnapshotLeadershipLoss(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()
	r.Advance()

	if err := r.CompactPrefix(
		r.hard.Commit,
		r.hard.Term,
	); err != nil {
		t.Fatal(err)
	}

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("expected eligible leader")
	}

	r.becomeFollower(r.hard.Term+1, 0)

	index, err := r.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitSnapshotNoStateMutation(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()
	r.Advance()

	if err := r.CompactPrefix(
		r.hard.Commit,
		r.hard.Term,
	); err != nil {
		t.Fatal(err)
	}

	beforeSnapshot := r.SnapshotMetadata()
	beforeHard := r.hard
	beforeVolatile := r.volatile
	beforeLog := cloneEntries(r.log)
	beforeReady := r.Ready()
	beforeReadID := r.nextReadID
	beforePending := r.pendingReadCount()

	for i := 0; i < 100; i++ {
		if _, err := r.readCommitBarrier(); err != nil {
			t.Fatal(err)
		}
	}

	if r.SnapshotMetadata() != beforeSnapshot ||
		r.hard != beforeHard ||
		r.volatile != beforeVolatile ||
		!reflect.DeepEqual(r.log, beforeLog) ||
		!reflect.DeepEqual(r.Ready(), beforeReady) ||
		r.nextReadID != beforeReadID ||
		r.pendingReadCount() != beforePending {
		t.Fatal("read guard modified compacted Raft state")
	}
}
