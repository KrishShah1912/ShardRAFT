package raft

import (
	"reflect"
	"testing"
)

func tickReadRetryCycle(r *Raft) {
	for i := 0; i < r.heartbeatTimeout; i++ {
		r.Tick()
	}
}

func collectReadRetryProbes(
	r *Raft,
	requestID uint64,
) []Message {
	var probes []Message

	for _, msg := range r.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID != 0 &&
			(requestID == 0 ||
				msg.RequestID == requestID) {
			probes = append(probes, msg)
		}
	}

	return probes
}

func TestReadRetryDroppedInitialProbes(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "dropped")

	initial := collectReadRetryProbes(r, request.id)

	if len(initial) != 2 {
		t.Fatal("expected two initial probes")
	}

	// Drop all initial messages.
	r.Advance()

	tickReadRetryCycle(r)

	retried := collectReadRetryProbes(r, request.id)

	if len(retried) != 2 {
		t.Fatalf("expected two retries, got %d", len(retried))
	}

	if !reflect.DeepEqual(initial, retried) {
		t.Fatal("retry changed probe identity")
	}
}

func TestReadRetryBeforeHeartbeatTimeout(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "timeout")

	r.Advance()

	for i := 0; i < r.heartbeatTimeout-1; i++ {
		r.Tick()

		if len(collectReadRetryProbes(r, request.id)) != 0 {
			t.Fatal("read retried before heartbeat timeout")
		}
	}

	r.Tick()

	if len(collectReadRetryProbes(r, request.id)) != 2 {
		t.Fatal("heartbeat timeout did not trigger retry")
	}
}

func TestReadRetryPreservesIdentity(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "identity")

	id := request.id
	term := request.term
	index := request.index

	r.Advance()

	for i := 0; i < 20; i++ {
		tickReadRetryCycle(r)

		if r.pendingReads[id] != request ||
			request.id != id ||
			request.term != term ||
			request.index != index ||
			string(request.context) != "identity" ||
			r.nextReadID != id {
			t.Fatal("retry modified request identity")
		}

		r.Advance()
	}
}

func TestReadRetrySkipsAcknowledgedPeers(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3, 4, 5},
	)

	request := startAutomaticRead(t, r, "partial")

	r.Advance()

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, request, 2),
	)

	if r.completedReadCount() != 0 {
		t.Fatal("read completed without majority")
	}

	tickReadRetryCycle(r)

	probes := collectReadRetryProbes(r, request.id)

	if len(probes) != 3 {
		t.Fatalf("expected three retries, got %d", len(probes))
	}

	for _, msg := range probes {
		if msg.To == 2 {
			t.Fatal("acknowledged peer received retry")
		}
	}
}

func TestReadRetryStopsAfterCompletion(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "completed")

	r.Advance()

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, request, 2),
	)

	if r.completedReadCount() != 1 {
		t.Fatal("read did not complete")
	}

	for i := 0; i < 20; i++ {
		tickReadRetryCycle(r)

		if len(collectReadRetryProbes(r, request.id)) != 0 {
			t.Fatal("completed read was retried")
		}

		r.Advance()
	}
}

func TestReadRetryStopsAfterLeadershipLoss(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "step-down")

	r.becomeFollower(r.hard.Term+1, 2)
	r.Advance()

	r.retryPendingReads()

	if len(collectReadRetryProbes(r, request.id)) != 0 {
		t.Fatal("former leader retried obsolete request")
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 {
		t.Fatal("obsolete read survived leadership loss")
	}
}

func TestReadRetryConcurrentRequests(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	first := startAutomaticRead(t, r, "first")
	second := startAutomaticRead(t, r, "second")

	r.Advance()

	tickReadRetryCycle(r)

	if len(collectReadRetryProbes(r, first.id)) != 2 ||
		len(collectReadRetryProbes(r, second.id)) != 2 {
		t.Fatal("concurrent reads were not retried independently")
	}

	r.Advance()

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, second, 2),
	)

	tickReadRetryCycle(r)

	if len(collectReadRetryProbes(r, first.id)) != 2 {
		t.Fatal("remaining request was not retried")
	}

	if len(collectReadRetryProbes(r, second.id)) != 0 {
		t.Fatal("completed request was retried")
	}
}

func TestReadRetryNoDuplicateQueuedProbes(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "bounded")

	// Do not Advance: the original probes remain queued.
	for i := 0; i < 100; i++ {
		r.retryPendingReads()
	}

	probes := collectReadRetryProbes(r, request.id)

	if len(probes) != 2 {
		t.Fatalf("duplicate probes accumulated: %d", len(probes))
	}
}

func TestReadRetryDeterministicOrdering(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	for i := 0; i < 20; i++ {
		startAutomaticRead(t, r, "ordered")
	}

	r.Advance()

	tickReadRetryCycle(r)

	probes := collectReadRetryProbes(r, 0)

	if len(probes) != 40 {
		t.Fatal("unexpected retry count")
	}

	for i := 1; i < len(probes); i++ {
		if probes[i].RequestID < probes[i-1].RequestID {
			t.Fatal("retry order is nondeterministic")
		}
	}
}

func TestReadRetryIsolatedLeader(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "isolated")

	r.Advance()

	for i := 0; i < 100; i++ {
		tickReadRetryCycle(r)

		if r.completedReadCount() != 0 ||
			r.pendingReads[request.id] != request {
			t.Fatal("isolated leader confirmed read")
		}

		r.Advance()
	}
}

func TestReadRetryPreservesCapturedIndex(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "captured")

	captured := request.index

	r.Advance()

	if err := r.Propose([]byte("new-command")); err != nil {
		t.Fatal(err)
	}

	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()
	r.Advance()

	if r.hard.Commit <= captured {
		t.Fatal("commit index did not advance")
	}

	tickReadRetryCycle(r)

	if request.index != captured {
		t.Fatal("retry changed captured read index")
	}

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, request, 2),
	)

	if r.completedReadCount() != 1 ||
		r.completedReads[0].Index != captured {
		t.Fatal("completion changed original read barrier")
	}
}

func TestReadRetryManualCompatibility(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := registerReadCompletionRequest(
		t, r, "manual",
	)

	r.Advance()

	tickReadRetryCycle(r)

	if len(collectReadRetryProbes(r, request.id)) != 0 {
		t.Fatal("manual request was automatically retried")
	}
}

func TestReadRetrySingleNode(t *testing.T) {
	r := newReadAPILeader(t, nil)

	if err := r.ReadIndex([]byte("single")); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		tickReadRetryCycle(r)
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 1 ||
		len(collectReadRetryProbes(r, 0)) != 0 {
		t.Fatal("single-node retry behavior is incorrect")
	}
}

func TestReadRetryActualFollowerRecovery(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	request := startAutomaticRead(
		t, leader, "recover",
	)

	// Drop the initial heartbeat probes.
	leader.Advance()

	tickReadRetryCycle(leader)

	var probe Message
	found := false

	for _, msg := range collectReadRetryProbes(
		leader, request.id,
	) {
		if msg.To == 2 {
			probe = msg
			found = true
			break
		}
	}

	if !found {
		t.Fatal("retry probe missing")
	}

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], probe,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 1 ||
		leader.pendingReadCount() != 0 {
		t.Fatal("read did not recover after dropped probes")
	}
}

func TestReadRetryPreservesOrdinaryHeartbeats(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := startAutomaticRead(t, r, "ordinary")

	r.Advance()

	tickReadRetryCycle(r)

	ordinary := 0
	correlated := 0
	appends := 0

	for _, msg := range r.Ready().Messages {
		switch msg.Type {
		case MsgHeartbeat:
			if msg.RequestID == 0 {
				ordinary++
			} else if msg.RequestID == request.id {
				correlated++
			}
		case MsgAppend:
			appends++
		}
	}

	if ordinary != 2 ||
		correlated != 2 ||
		appends != 2 {
		t.Fatalf(
			"unexpected messages: ordinary=%d read=%d append=%d",
			ordinary, correlated, appends,
		)
	}
}

func TestReadRetryNoDurableChanges(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	startAutomaticRead(t, r, "no-durable-changes")

	r.Advance()

	beforeHard := r.hard
	beforeVolatile := r.volatile
	beforeLog := cloneEntries(r.log)
	beforeSnapshot := r.SnapshotMetadata()
	beforeAppendID := r.nextRequestID
	beforeReadID := r.nextReadID

	for i := 0; i < 100; i++ {
		r.retryPendingReads()
	}

	if r.hard != beforeHard ||
		r.volatile != beforeVolatile ||
		!reflect.DeepEqual(r.log, beforeLog) ||
		r.SnapshotMetadata() != beforeSnapshot ||
		r.nextRequestID != beforeAppendID ||
		r.nextReadID != beforeReadID {
		t.Fatal("read retry modified consensus state")
	}

	rd := r.Ready()

	if rd.HardState != nil ||
		rd.TruncateFrom != 0 ||
		len(rd.Entries) != 0 ||
		len(rd.CommittedEntries) != 0 {
		t.Fatal("read retry generated durable work")
	}
}
