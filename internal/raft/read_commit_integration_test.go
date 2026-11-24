package raft

import (
	"errors"
	"testing"
)

// Replicate the leader's current log to a real follower
// and deliver its actual AppendEntries response.
func commitReadIntegrationNoop(
	t *testing.T,
	leader *Raft,
	follower *Raft,
) {
	t.Helper()

	leader.sendAppend(follower.id)

	var request Message
	found := false

	for _, msg := range leader.Ready().Messages {
		if msg.Type == MsgAppend &&
			msg.To == follower.id {
			request = msg
			found = true
		}
	}

	if !found {
		t.Fatal("expected AppendEntries request")
	}

	leader.Advance()

	if err := follower.Step(request); err != nil {
		t.Fatal(err)
	}

	var response Message
	found = false

	for _, msg := range follower.Ready().Messages {
		if msg.Type == MsgAppendResp &&
			msg.RequestID == request.RequestID {
			response = msg
			found = true
		}
	}

	if !found || response.Reject {
		t.Fatal("expected successful replication response")
	}

	follower.Advance()

	if err := leader.Step(response); err != nil {
		t.Fatal(err)
	}

	if !leader.hasCommittedCurrentTerm() {
		t.Fatal("current-term entry was not committed")
	}

	leader.Advance()
}

func TestReadCommitIntegrationQuorumWithoutCommit(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	request := registerReadHeartbeatRequest(t, leader)

	messages := collectReadHeartbeatRequests(
		t, leader, request,
	)

	reply := deliverReadHeartbeatRequest(
		t,
		followers[messages[0].To],
		messages[0],
	)

	stepReadHeartbeatAck(t, leader, reply)

	if !leader.hasReadQuorum(request) {
		t.Fatal("expected heartbeat quorum")
	}

	if leader.hasCommittedCurrentTerm() {
		t.Fatal("heartbeat quorum incorrectly enabled commit guard")
	}

	index, err := leader.readCommitBarrier()

	if index != 0 || !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}
}

func TestReadCommitIntegrationCommitWithoutQuorum(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	index, err := leader.readCommitBarrier()

	if err != nil {
		t.Fatal(err)
	}

	request, err := leader.registerPendingRead(
		[]byte("commit-without-quorum"),
		index,
	)

	if err != nil {
		t.Fatal(err)
	}

	collectReadHeartbeatRequests(t, leader, request)

	// No heartbeat responses are delivered.
	if leader.hasReadQuorum(request) {
		t.Fatal("commit alone satisfied read quorum")
	}
}

func TestReadCommitIntegrationBothConditions(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	index, err := leader.readCommitBarrier()

	if err != nil || index == 0 {
		t.Fatalf("unexpected barrier: %d, %v", index, err)
	}

	request, err := leader.registerPendingRead(
		[]byte("both-conditions"),
		index,
	)

	if err != nil {
		t.Fatal(err)
	}

	beforeLast := leader.lastIndex()
	beforeHard := leader.hard

	messages := collectReadHeartbeatRequests(
		t, leader, request,
	)

	reply := deliverReadHeartbeatRequest(
		t,
		followers[messages[0].To],
		messages[0],
	)

	stepReadHeartbeatAck(t, leader, reply)

	if !leader.hasCommittedCurrentTerm() ||
		!leader.hasReadQuorum(request) {
		t.Fatal("expected both safety conditions")
	}

	if request.index != index {
		t.Fatal("captured read barrier changed")
	}

	if leader.lastIndex() != beforeLast ||
		leader.hard != beforeHard {
		t.Fatal("read confirmation modified durable state")
	}

	if leader.pendingReadCount() != 1 {
		t.Fatal("read completed before completion integration")
	}
}

func TestReadCommitIntegrationFreshRound(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	oldRequest := registerReadHeartbeatRequest(t, leader)

	oldMessages := collectReadHeartbeatRequests(
		t, leader, oldRequest,
	)

	oldReply := deliverReadHeartbeatRequest(
		t,
		followers[oldMessages[0].To],
		oldMessages[0],
	)

	stepReadHeartbeatAck(t, leader, oldReply)

	if !leader.hasReadQuorum(oldRequest) {
		t.Fatal("expected old heartbeat quorum")
	}

	leader.removePendingRead(oldRequest.id)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	index, err := leader.readCommitBarrier()

	if err != nil {
		t.Fatal(err)
	}

	newRequest, err := leader.registerPendingRead(
		[]byte("fresh-round"),
		index,
	)

	if err != nil {
		t.Fatal(err)
	}

	messages := collectReadHeartbeatRequests(
		t, leader, newRequest,
	)

	if oldRequest.id == newRequest.id {
		t.Fatal("read request ID was reused")
	}

	stepReadHeartbeatAck(t, leader, oldReply)

	if leader.hasReadQuorum(newRequest) {
		t.Fatal("old response satisfied fresh read quorum")
	}

	reply := deliverReadHeartbeatRequest(
		t,
		followers[messages[0].To],
		messages[0],
	)

	stepReadHeartbeatAck(t, leader, reply)

	if !leader.hasReadQuorum(newRequest) {
		t.Fatal("fresh response did not confirm quorum")
	}
}

func TestReadCommitIntegrationLeadershipLoss(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	index, err := leader.readCommitBarrier()

	if err != nil {
		t.Fatal(err)
	}

	request, err := leader.registerPendingRead(
		[]byte("leadership-loss"),
		index,
	)

	if err != nil {
		t.Fatal(err)
	}

	messages := collectReadHeartbeatRequests(
		t, leader, request,
	)

	reply := deliverReadHeartbeatRequest(
		t,
		followers[messages[0].To],
		messages[0],
	)

	stepReadHeartbeatAck(t, leader, reply)

	if !leader.hasReadQuorum(request) {
		t.Fatal("expected confirmed quorum")
	}

	leader.becomeFollower(leader.hard.Term+1, 2)

	if leader.hasCommittedCurrentTerm() {
		t.Fatal("former leader retained commit eligibility")
	}

	if leader.hasReadQuorum(request) {
		t.Fatal("former leader retained read quorum")
	}

	barrier, err := leader.readCommitBarrier()

	if barrier != 0 || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected barrier: %d, %v", barrier, err)
	}
}

func TestReadCommitIntegrationCompactedSingleNode(t *testing.T) {
	r := NewRaft(1, nil, 5)
	r.startElection()
	r.Advance()

	index := r.hard.Commit
	term := r.hard.Term

	if err := r.CompactPrefix(index, term); err != nil {
		t.Fatal(err)
	}

	if len(r.log) != 0 {
		t.Fatal("expected compacted log")
	}

	barrier, err := r.readCommitBarrier()

	if err != nil || barrier != index {
		t.Fatalf("unexpected barrier: %d, %v", barrier, err)
	}

	request, err := r.registerPendingRead(
		[]byte("compacted-read"),
		barrier,
	)

	if err != nil {
		t.Fatal(err)
	}

	r.sendReadHeartbeats(request)

	if !r.hasReadQuorum(request) {
		t.Fatal("single-node quorum not confirmed")
	}

	if len(r.Ready().Messages) != 0 {
		t.Fatal("single-node read generated network traffic")
	}

	if len(r.log) != 0 ||
		r.hard.Commit != index ||
		r.snapshotIndex != index {
		t.Fatal("read confirmation changed compacted state")
	}
}
