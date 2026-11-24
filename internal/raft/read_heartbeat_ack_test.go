package raft

import (
	"reflect"
	"testing"
)

func readHeartbeatAck(
	r *Raft,
	request *pendingRead,
	peer NodeID,
) Message {
	return Message{
		Type:      MsgHeartbeatResp,
		From:      peer,
		To:        r.id,
		Term:      request.term,
		RequestID: request.id,
	}
}

func stepReadHeartbeatAck(
	t *testing.T,
	r *Raft,
	msg Message,
) {
	t.Helper()

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}
}

func TestReadHeartbeatAckValidResponse(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	msg := readHeartbeatAck(r, request, 2)

	stepReadHeartbeatAck(t, r, msg)

	if len(request.acks) != 2 {
		t.Fatal("valid acknowledgment was not recorded")
	}

	if _, exists := request.acks[2]; !exists {
		t.Fatal("missing follower acknowledgment")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("acknowledgment prematurely completed read")
	}
}

func TestReadHeartbeatAckDuplicateResponse(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	msg := readHeartbeatAck(r, request, 2)

	for i := 0; i < 100; i++ {
		stepReadHeartbeatAck(t, r, msg)
	}

	if len(request.acks) != 2 {
		t.Fatal("duplicate acknowledgment counted more than once")
	}
}

func TestReadHeartbeatAckDistinctVoters(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3, 4, 5},
	)

	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	for _, peer := range []NodeID{2, 3, 4, 5} {
		stepReadHeartbeatAck(
			t,
			r,
			readHeartbeatAck(r, request, peer),
		)
	}

	if len(request.acks) != 5 {
		t.Fatal("incorrect distinct voter count")
	}
}

func TestReadHeartbeatAckUnknownPeer(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	before := r.Status()

	msg := readHeartbeatAck(r, request, 99)
	msg.Term += 100

	stepReadHeartbeatAck(t, r, msg)

	if len(request.acks) != 1 {
		t.Fatal("unknown peer acknowledgment counted")
	}

	if r.Status() != before {
		t.Fatal("unknown peer changed leader status")
	}
}

func TestReadHeartbeatAckInvalidSender(t *testing.T) {
	for _, peer := range []NodeID{0, 1} {
		r := newReadHeartbeatLeader(t, []NodeID{2, 3})
		request := registerReadHeartbeatRequest(t, r)
		r.sendReadHeartbeats(request)

		before := r.Status()

		msg := readHeartbeatAck(r, request, peer)
		msg.Term += 100

		stepReadHeartbeatAck(t, r, msg)

		if len(request.acks) != 1 {
			t.Fatal("invalid sender acknowledgment counted")
		}

		if r.Status() != before {
			t.Fatal("invalid sender changed leader status")
		}
	}
}

func TestReadHeartbeatAckWrongDestination(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	msg := readHeartbeatAck(r, request, 2)
	msg.To = 3

	if err := r.Step(msg); err == nil {
		t.Fatal("wrong destination accepted")
	}

	if len(request.acks) != 1 {
		t.Fatal("wrong destination acknowledgment counted")
	}
}

func TestReadHeartbeatAckStaleTerm(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	msg := readHeartbeatAck(r, request, 2)
	msg.Term--

	stepReadHeartbeatAck(t, r, msg)

	if len(request.acks) != 1 {
		t.Fatal("stale term acknowledgment counted")
	}
}

func TestReadHeartbeatAckHigherTerm(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	msg := readHeartbeatAck(r, request, 2)
	msg.Term++

	stepReadHeartbeatAck(t, r, msg)

	if r.Status().Role != Follower {
		t.Fatal("leader did not step down")
	}

	if r.Status().Term != msg.Term {
		t.Fatal("higher term was not adopted")
	}

	if len(request.acks) != 1 {
		t.Fatal("higher term acknowledgment counted")
	}
}

func TestReadHeartbeatAckRejectedResponse(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	msg := readHeartbeatAck(r, request, 2)
	msg.Reject = true

	stepReadHeartbeatAck(t, r, msg)

	if len(request.acks) != 1 {
		t.Fatal("rejected response counted")
	}
}

func TestReadHeartbeatAckUnknownRequest(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	msg := readHeartbeatAck(r, request, 2)
	msg.RequestID += 100

	stepReadHeartbeatAck(t, r, msg)

	if len(request.acks) != 1 {
		t.Fatal("unknown request acknowledgment counted")
	}
}

func TestReadHeartbeatAckZeroRequestID(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	msg := readHeartbeatAck(r, request, 2)
	msg.RequestID = 0

	stepReadHeartbeatAck(t, r, msg)

	if len(request.acks) != 1 {
		t.Fatal("zero request ID acknowledgment counted")
	}
}

func TestReadHeartbeatAckRemovedRequest(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	r.removePendingRead(request.id)

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, request, 2),
	)

	if len(request.acks) != 1 {
		t.Fatal("removed request was acknowledged")
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("removed request was recreated")
	}
}

func TestReadHeartbeatAckNonLeader(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

	r.becomeFollower(r.hard.Term, 2)

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, request, 2),
	)

	if len(request.acks) != 1 {
		t.Fatal("nonleader recorded acknowledgment")
	}
}

func TestReadHeartbeatAckConcurrentRequests(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})

	first := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(first)
	second := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(second)

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, first, 2),
	)

	if len(first.acks) != 2 ||
		len(second.acks) != 1 {
		t.Fatal("acknowledgment leaked between requests")
	}

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, second, 3),
	)

	if _, exists := first.acks[3]; exists {
		t.Fatal("second request modified first request")
	}

	if _, exists := second.acks[2]; exists {
		t.Fatal("first request modified second request")
	}

	if len(second.acks) != 2 {
		t.Fatal("second request acknowledgment missing")
	}
}

func TestReadHeartbeatAckMalformedPayload(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Message)
	}{
		{
			name: "commit",
			mutate: func(m *Message) {
				m.Commit = 100
			},
		},
		{
			name: "index",
			mutate: func(m *Message) {
				m.Index = 10
			},
		},
		{
			name: "log-term",
			mutate: func(m *Message) {
				m.LogTerm = 5
			},
		},
		{
			name: "entries",
			mutate: func(m *Message) {
				m.Entries = []Entry{
					{Index: 1, Term: 1},
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReadHeartbeatLeader(
				t,
				[]NodeID{2, 3},
			)

			request := registerReadHeartbeatRequest(t, r)

			r.sendReadHeartbeats(request)

			msg := readHeartbeatAck(r, request, 2)
			tc.mutate(&msg)

			stepReadHeartbeatAck(t, r, msg)

			if len(request.acks) != 1 {
				t.Fatal("malformed acknowledgment counted")
			}
		})
	}
}

func TestReadHeartbeatAckPreservesReplicationState(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)
	r.sendReadHeartbeats(request)

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

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, request, 2),
	)

	if r.hard != beforeHard {
		t.Fatal("acknowledgment modified HardState")
	}

	if !reflect.DeepEqual(beforeLog, r.log) {
		t.Fatal("acknowledgment modified log")
	}

	if !reflect.DeepEqual(beforeNext, r.nextIndex) {
		t.Fatal("acknowledgment modified nextIndex")
	}

	if !reflect.DeepEqual(beforeMatch, r.matchIndex) {
		t.Fatal("acknowledgment modified matchIndex")
	}

	if !reflect.DeepEqual(beforePending, r.pendingAppend) {
		t.Fatal("acknowledgment modified pending appends")
	}

	if r.nextRequestID != beforeRequestID {
		t.Fatal("acknowledgment modified append request sequence")
	}

	ready := r.Ready()

	if len(ready.Entries) != 0 ||
		ready.HardState != nil ||
		ready.TruncateFrom != 0 {
		t.Fatal("acknowledgment generated durable work")
	}
}
