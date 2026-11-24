package raft

import (
	"reflect"
	"testing"
)

func newReadHeartbeatLeader(
	t *testing.T,
	peers []NodeID,
) *Raft {
	t.Helper()

	r := NewRaft(1, peers, 5)

	r.startElection()

	for _, peer := range peers {
		if r.Status().Role == Leader {
			break
		}

		err := r.Step(Message{
			Type: MsgVoteResp,
			From: peer,
			To:   r.id,
			Term: r.hard.Term,
		})

		if err != nil {
			t.Fatal(err)
		}
	}

	if r.Status().Role != Leader {
		t.Fatal("node did not become leader")
	}

	r.Advance()

	return r
}

func registerReadHeartbeatRequest(
	t *testing.T,
	r *Raft,
) *pendingRead {
	t.Helper()

	request, err := r.registerPendingRead(
		[]byte("heartbeat-read"),
		r.Status().Commit,
	)

	if err != nil {
		t.Fatal(err)
	}

	return request
}

func TestReadHeartbeatTransmission(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3},
	)

	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	messages := r.Ready().Messages

	if len(messages) != 2 {
		t.Fatalf(
			"expected two heartbeats, got %d",
			len(messages),
		)
	}

	for i, msg := range messages {
		if msg.Type != MsgHeartbeat {
			t.Fatal("incorrect message type")
		}

		if msg.From != r.id {
			t.Fatal("incorrect sender")
		}

		if msg.To != NodeID(i+2) {
			t.Fatal("incorrect destination")
		}

		if msg.Term != request.term {
			t.Fatal("incorrect term")
		}

		if msg.RequestID != request.id {
			t.Fatal("incorrect read request ID")
		}

		if msg.Commit != 0 ||
			msg.Index != 0 ||
			msg.LogTerm != 0 ||
			len(msg.Entries) != 0 ||
			msg.Reject {
			t.Fatal("unexpected heartbeat payload")
		}
	}
}

func TestReadHeartbeatDistinctRequestIDs(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3},
	)

	first := registerReadHeartbeatRequest(t, r)
	second := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(first)
	r.sendReadHeartbeats(second)

	counts := make(map[uint64]int)

	for _, msg := range r.Ready().Messages {
		counts[msg.RequestID]++
	}

	if first.id == second.id {
		t.Fatal("duplicate request IDs")
	}

	if counts[first.id] != 2 ||
		counts[second.id] != 2 {
		t.Fatal("incorrect heartbeat correlation")
	}
}

func TestReadHeartbeatSkipsAcknowledgedPeers(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3, 4, 5},
	)

	request := registerReadHeartbeatRequest(t, r)

	request.acks[2] = struct{}{}

	r.sendReadHeartbeats(request)

	messages := r.Ready().Messages

	if len(messages) != 3 {
		t.Fatalf(
			"expected three heartbeats, got %d",
			len(messages),
		)
	}

	for _, msg := range messages {
		if msg.To == 2 {
			t.Fatal("heartbeat sent to acknowledged peer")
		}
	}
}

func TestReadHeartbeatStopsAfterQuorum(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3},
	)

	request := registerReadHeartbeatRequest(t, r)

	request.acks[2] = struct{}{}

	r.sendReadHeartbeats(request)

	if len(r.Ready().Messages) != 0 {
		t.Fatal("heartbeat sent after quorum confirmation")
	}
}

func TestReadHeartbeatSingleNode(t *testing.T) {
	r := newReadHeartbeatLeader(t, nil)

	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	if len(r.Ready().Messages) != 0 {
		t.Fatal("single-node leader generated network traffic")
	}
}

func TestReadHeartbeatRejectInactiveRequest(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3},
	)

	request := registerReadHeartbeatRequest(t, r)

	r.removePendingRead(request.id)

	r.sendReadHeartbeats(request)
	r.sendReadHeartbeats(nil)

	if len(r.Ready().Messages) != 0 {
		t.Fatal("inactive request generated heartbeats")
	}
}

func TestReadHeartbeatRejectStaleTerm(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3},
	)

	request := registerReadHeartbeatRequest(t, r)

	request.term--

	r.sendReadHeartbeats(request)

	if len(r.Ready().Messages) != 0 {
		t.Fatal("stale request generated heartbeats")
	}
}

func TestReadHeartbeatRejectNonLeader(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3},
	)

	request := registerReadHeartbeatRequest(t, r)

	r.becomeFollower(r.hard.Term+1, 2)

	r.sendReadHeartbeats(request)

	if len(r.Ready().Messages) != 0 {
		t.Fatal("follower generated read heartbeats")
	}
}

func TestReadHeartbeatPreservesReplicationState(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3},
	)

	request := registerReadHeartbeatRequest(t, r)

	beforeHard := r.hard
	beforeLog := cloneEntries(r.log)
	beforeRequestID := r.nextRequestID

	beforeNext := make(map[NodeID]uint64)
	beforeMatch := make(map[NodeID]uint64)
	beforePending := make(map[NodeID]map[uint64]uint64)

	for peer, index := range r.nextIndex {
		beforeNext[peer] = index
	}

	for peer, index := range r.matchIndex {
		beforeMatch[peer] = index
	}

	for peer, pending := range r.pendingAppend {
		beforePending[peer] = make(map[uint64]uint64)

		for id, index := range pending {
			beforePending[peer][id] = index
		}
	}

	r.sendReadHeartbeats(request)

	if r.hard != beforeHard {
		t.Fatal("read heartbeat changed HardState")
	}

	if !reflect.DeepEqual(beforeLog, r.log) {
		t.Fatal("read heartbeat changed log")
	}

	if !reflect.DeepEqual(beforeNext, r.nextIndex) {
		t.Fatal("read heartbeat changed nextIndex")
	}

	if !reflect.DeepEqual(beforeMatch, r.matchIndex) {
		t.Fatal("read heartbeat changed matchIndex")
	}

	if !reflect.DeepEqual(beforePending, r.pendingAppend) {
		t.Fatal("read heartbeat changed pending appends")
	}

	if r.nextRequestID != beforeRequestID {
		t.Fatal("read heartbeat changed append request sequence")
	}

	ready := r.Ready()

	if len(ready.Entries) != 0 ||
		ready.HardState != nil ||
		ready.TruncateFrom != 0 {
		t.Fatal("read heartbeat generated durable changes")
	}
}

func TestReadHeartbeatOrdinaryHeartbeatCompatibility(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3},
	)

	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)
	r.sendHeartbeats()

	var ordinary, correlated, appends int

	for _, msg := range r.Ready().Messages {
		switch msg.Type {
		case MsgHeartbeat:
			if msg.RequestID == 0 {
				ordinary++
			} else if msg.RequestID == request.id {
				correlated++
			} else {
				t.Fatal("unexpected heartbeat request ID")
			}

		case MsgAppend:
			appends++
		}
	}

	if ordinary != 2 ||
		correlated != 2 ||
		appends != 2 {
		t.Fatalf(
			"unexpected messages: ordinary=%d correlated=%d appends=%d",
			ordinary,
			correlated,
			appends,
		)
	}
}
