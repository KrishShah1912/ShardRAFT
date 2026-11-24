package raft

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func lifecycleProbe(
	t *testing.T,
	r *Raft,
	id uint64,
	peer NodeID,
) Message {
	t.Helper()

	for _, msg := range r.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID == id &&
			msg.To == peer {
			return msg
		}
	}

	t.Fatalf(
		"missing read probe: request=%d peer=%d",
		id, peer,
	)

	return Message{}
}

func TestReadLifecycleThreeNodes(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	request := startAutomaticRead(
		t, leader, "three-node-lifecycle",
	)

	probe := lifecycleProbe(
		t, leader, request.id, 2,
	)

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], probe,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.pendingReadCount() != 0 ||
		leader.completedReadCount() != 1 {
		t.Fatal("read lifecycle did not complete")
	}

	state := leader.completedReads[0]

	if state.RequestID != request.id ||
		state.Term != request.term ||
		state.Index != request.index ||
		string(state.Context) !=
			"three-node-lifecycle" {
		t.Fatalf("incorrect read state: %+v", state)
	}
}

func TestReadLifecycleFiveNodes(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 5)

	leader.sendAppend(2)
	leader.sendAppend(3)

	messages := leader.Ready().Messages

	leader.Advance()

	for _, msg := range messages {
		if msg.Type != MsgAppend ||
			(msg.To != 2 && msg.To != 3) {
			continue
		}

		follower := followers[msg.To]

		if err := follower.Step(msg); err != nil {
			t.Fatal(err)
		}

		replies := follower.Ready().Messages

		follower.Advance()

		for _, reply := range replies {
			if err := leader.Step(reply); err != nil {
				t.Fatal(err)
			}
		}
	}

	if !leader.hasCommittedCurrentTerm() {
		t.Fatal("current-term no-op not committed")
	}

	leader.Advance()

	request := startAutomaticRead(
		t, leader, "five-node-lifecycle",
	)

	first := lifecycleProbe(
		t, leader, request.id, 2,
	)

	second := lifecycleProbe(
		t, leader, request.id, 3,
	)

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], first,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 0 {
		t.Fatal("read completed without majority")
	}

	reply = deliverReadHeartbeatRequest(
		t, followers[3], second,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 1 ||
		leader.pendingReadCount() != 0 {
		t.Fatal("five-node majority did not complete")
	}
}

func TestReadLifecycleDroppedResponse(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	request := startAutomaticRead(
		t, leader, "dropped-response",
	)

	probe := lifecycleProbe(
		t, leader, request.id, 2,
	)

	leader.Advance()

	// Deliver the probe but discard its response.
	deliverReadHeartbeatRequest(
		t, followers[2], probe,
	)

	if leader.completedReadCount() != 0 {
		t.Fatal("undelivered response completed read")
	}

	tickReadRetryCycle(leader)

	retry := lifecycleProbe(
		t, leader, request.id, 2,
	)

	if !reflect.DeepEqual(probe, retry) {
		t.Fatal("retry changed request identity")
	}

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], retry,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 1 ||
		leader.pendingReadCount() != 0 {
		t.Fatal("read did not recover")
	}
}

func TestReadLifecycleConcurrentOutOfOrder(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	const count = 32

	requests := make([]*pendingRead, count)
	probes := make([]Message, count)

	for i := 0; i < count; i++ {
		requests[i] = startAutomaticRead(
			t, leader, fmt.Sprintf("read-%d", i),
		)

		probes[i] = lifecycleProbe(
			t, leader, requests[i].id, 2,
		)
	}

	leader.Advance()

	for i := count - 1; i >= 0; i-- {
		reply := deliverReadHeartbeatRequest(
			t, followers[2], probes[i],
		)

		stepReadHeartbeatAck(t, leader, reply)

		// Duplicate delivery must not duplicate completion.
		stepReadHeartbeatAck(t, leader, reply)

		position := count - 1 - i

		if leader.completedReadCount() != position+1 {
			t.Fatal("incorrect completion count")
		}

		state := leader.completedReads[position]

		if state.RequestID != requests[i].id ||
			state.Index != requests[i].index ||
			string(state.Context) !=
				fmt.Sprintf("read-%d", i) {
			t.Fatal("concurrent read identity mixed")
		}
	}

	if leader.pendingReadCount() != 0 ||
		leader.completedReadCount() != count {
		t.Fatal("concurrent lifecycle incomplete")
	}
}

func TestReadLifecycleCommitAdmission(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	if err := leader.ReadIndex(
		[]byte("before-commit"),
	); !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected admission result: %v", err)
	}

	if leader.nextReadID != 0 ||
		leader.pendingReadCount() != 0 ||
		leader.completedReadCount() != 0 {
		t.Fatal("failed admission allocated read state")
	}

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	request := startAutomaticRead(
		t, leader, "after-commit",
	)

	probe := lifecycleProbe(
		t, leader, request.id, 2,
	)

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], probe,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 1 ||
		request.id != 1 {
		t.Fatal("read admission retry failed")
	}
}

func TestReadLifecycleCapturedBarrier(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	first := startAutomaticRead(
		t, leader, "first-barrier",
	)

	firstIndex := first.index

	leader.Advance()

	if err := leader.Propose(
		[]byte("new-command"),
	); err != nil {
		t.Fatal(err)
	}

	leader.sendAppend(2)

	var appendMessage Message
	found := false

	for _, msg := range leader.Ready().Messages {
		if msg.Type == MsgAppend && msg.To == 2 {
			appendMessage = msg
			found = true
		}
	}

	if !found {
		t.Fatal("append message missing")
	}

	leader.Advance()

	if err := followers[2].Step(
		appendMessage,
	); err != nil {
		t.Fatal(err)
	}

	replies := followers[2].Ready().Messages

	followers[2].Advance()

	for _, reply := range replies {
		if err := leader.Step(reply); err != nil {
			t.Fatal(err)
		}
	}

	if leader.hard.Commit <= firstIndex {
		t.Fatal("commit index did not advance")
	}

	leader.Advance()

	second := startAutomaticRead(
		t, leader, "second-barrier",
	)

	tickReadRetryCycle(leader)

	firstProbe := lifecycleProbe(
		t, leader, first.id, 2,
	)

	secondProbe := lifecycleProbe(
		t, leader, second.id, 2,
	)

	leader.Advance()

	for _, probe := range []Message{
		secondProbe, firstProbe,
	} {
		reply := deliverReadHeartbeatRequest(
			t, followers[2], probe,
		)

		stepReadHeartbeatAck(t, leader, reply)
	}

	if leader.completedReadCount() != 2 {
		t.Fatal("expected two completed reads")
	}

	if leader.completedReads[0].Index != second.index ||
		leader.completedReads[1].Index != firstIndex ||
		second.index <= firstIndex {
		t.Fatal("captured barriers changed")
	}
}

func TestReadLifecycleLeadershipLoss(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	first := startAutomaticRead(
		t, leader, "completed",
	)

	second := startAutomaticRead(
		t, leader, "pending",
	)

	firstProbe := lifecycleProbe(
		t, leader, first.id, 2,
	)

	secondProbe := lifecycleProbe(
		t, leader, second.id, 2,
	)

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], firstProbe,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 1 ||
		leader.pendingReadCount() != 1 {
		t.Fatal("expected mixed lifecycle state")
	}

	delayed := deliverReadHeartbeatRequest(
		t, followers[2], secondProbe,
	)

	leader.becomeFollower(
		leader.hard.Term+1, 2,
	)

	requireInvalidatedReads(t, leader)

	stepReadHeartbeatAck(t, leader, delayed)

	requireInvalidatedReads(t, leader)

	if err := leader.ReadIndex(
		[]byte("former-leader"),
	); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected admission result: %v", err)
	}
}

func TestReadLifecycleIsolatedLeader(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	request := startAutomaticRead(
		t, leader, "isolated",
	)

	leader.Advance()

	for i := 0; i < 100; i++ {
		tickReadRetryCycle(leader)

		if leader.completedReadCount() != 0 ||
			leader.pendingReads[request.id] != request {
			t.Fatal("isolated leader completed read")
		}

		leader.Advance()
	}

	tickReadRetryCycle(leader)

	probe := lifecycleProbe(
		t, leader, request.id, 2,
	)

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], probe,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 1 ||
		leader.pendingReadCount() != 0 {
		t.Fatal("read did not recover after reconnection")
	}
}

func TestReadLifecycleSingleNodeCompaction(t *testing.T) {
	r := newReadAPILeader(t, nil)

	index := r.hard.Commit

	term, exists := r.TermAt(index)

	if !exists {
		t.Fatal("commit term unavailable")
	}

	if err := r.CompactPrefix(index, term); err != nil {
		t.Fatal(err)
	}

	beforeLog := cloneEntries(r.log)
	beforeHard := r.hard
	beforeSnapshot := r.SnapshotMetadata()

	for i := 0; i < 100; i++ {
		if err := r.ReadIndex(
			[]byte(fmt.Sprintf("read-%d", i)),
		); err != nil {
			t.Fatal(err)
		}
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 100 {
		t.Fatal("compacted leader did not complete reads")
	}

	for _, state := range r.completedReads {
		if state.Index != index ||
			state.Term != term {
			t.Fatal("incorrect compacted read barrier")
		}
	}

	if !reflect.DeepEqual(beforeLog, r.log) ||
		r.hard != beforeHard ||
		r.SnapshotMetadata() != beforeSnapshot ||
		len(r.Ready().Messages) != 0 {
		t.Fatal("compacted reads changed consensus state")
	}
}

func TestReadLifecycleCapacityRecovery(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	for i := 0; i < maxPendingReads; i++ {
		startAutomaticRead(t, r, "capacity")
	}

	if err := r.ReadIndex(
		[]byte("overflow"),
	); !errors.Is(err, ErrTooManyPendingReads) {
		t.Fatalf("unexpected capacity result: %v", err)
	}

	oldID := r.nextReadID

	r.becomeFollower(r.hard.Term+1, 0)
	r.Advance()

	requireInvalidatedReads(t, r)

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

	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()
	r.Advance()

	request := startAutomaticRead(
		t, r, "recovered-capacity",
	)

	if request.id != oldID+1 ||
		r.pendingReadCount() != 1 {
		t.Fatal("capacity recovery reused identity")
	}
}

func TestReadLifecycleNoAdditionalLogEntries(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	beforeHard := leader.hard
	beforeLog := cloneEntries(leader.log)
	beforeSnapshot := leader.SnapshotMetadata()

	for i := 0; i < 100; i++ {
		request := startAutomaticRead(
			t, leader, "read-only",
		)

		probe := lifecycleProbe(
			t, leader, request.id, 2,
		)

		leader.Advance()

		reply := deliverReadHeartbeatRequest(
			t, followers[2], probe,
		)

		stepReadHeartbeatAck(t, leader, reply)
	}

	if leader.completedReadCount() != 100 ||
		leader.pendingReadCount() != 0 {
		t.Fatal("incorrect completion count")
	}

	if leader.hard != beforeHard ||
		!reflect.DeepEqual(leader.log, beforeLog) ||
		leader.SnapshotMetadata() != beforeSnapshot {
		t.Fatal("read lifecycle changed durable state")
	}

	rd := leader.Ready()

	if rd.HardState != nil ||
		rd.TruncateFrom != 0 ||
		len(rd.Entries) != 0 ||
		len(rd.CommittedEntries) != 0 {
		t.Fatal("read lifecycle generated durable work")
	}
}

func TestReadLifecycleMalformedThenValidResponse(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	request := startAutomaticRead(
		t, leader, "malformed-then-valid",
	)

	probe := lifecycleProbe(
		t, leader, request.id, 2,
	)

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], probe,
	)

	malformed := reply
	malformed.Term++
	malformed.Commit = 1

	beforeHard := leader.hard

	stepReadHeartbeatAck(t, leader, malformed)

	if leader.hard != beforeHard ||
		leader.volatile.Role != Leader ||
		leader.pendingReadCount() != 1 ||
		leader.completedReadCount() != 0 {
		t.Fatal("malformed response changed lifecycle")
	}

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 1 ||
		leader.pendingReadCount() != 0 {
		t.Fatal("valid response did not complete read")
	}
}
