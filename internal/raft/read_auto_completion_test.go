package raft

import (
	"reflect"
	"testing"
)

func startAutomaticRead(
	t *testing.T,
	r *Raft,
	ctx string,
) *pendingRead {
	t.Helper()

	if err := r.ReadIndex([]byte(ctx)); err != nil {
		t.Fatal(err)
	}

	request := r.pendingReads[r.nextReadID]

	if request == nil || !request.autoComplete {
		t.Fatal("expected automatic pending read")
	}

	return request
}

func TestReadAutoCompletionThreeNodes(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})
	request := startAutomaticRead(t, r, "three")

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, request, 2),
	)

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 1 {
		t.Fatal("majority did not complete read")
	}

	state := r.completedReads[0]

	if state.RequestID != request.id ||
		state.Term != request.term ||
		state.Index != request.index ||
		string(state.Context) != "three" {
		t.Fatalf("incorrect completed state: %+v", state)
	}
}

func TestReadAutoCompletionFiveNodes(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3, 4, 5},
	)

	request := startAutomaticRead(t, r, "five")

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, request, 2),
	)

	if r.completedReadCount() != 0 {
		t.Fatal("read completed without majority")
	}

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, request, 3),
	)

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 1 {
		t.Fatal("majority did not complete read")
	}
}

func TestReadAutoCompletionDuplicateResponses(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})
	request := startAutomaticRead(t, r, "duplicate")

	reply := readHeartbeatAck(r, request, 2)

	for i := 0; i < 100; i++ {
		stepReadHeartbeatAck(t, r, reply)
	}

	if r.completedReadCount() != 1 ||
		r.pendingReadCount() != 0 {
		t.Fatal("duplicate response changed completion state")
	}
}

func TestReadAutoCompletionDuplicateWithoutMajority(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3, 4, 5},
	)

	request := startAutomaticRead(t, r, "duplicate")

	reply := readHeartbeatAck(r, request, 2)

	for i := 0; i < 100; i++ {
		stepReadHeartbeatAck(t, r, reply)
	}

	if r.completedReadCount() != 0 ||
		r.pendingReadCount() != 1 {
		t.Fatal("duplicate acknowledgments formed majority")
	}
}

func TestReadAutoCompletionConcurrentRequests(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	first := startAutomaticRead(t, r, "first")
	second := startAutomaticRead(t, r, "second")

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, second, 2),
	)

	if r.completedReadCount() != 1 ||
		r.pendingReadCount() != 1 {
		t.Fatal("incorrect independent completion")
	}

	if r.pendingReads[first.id] != first {
		t.Fatal("first request was removed")
	}

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, first, 3),
	)

	if r.completedReadCount() != 2 ||
		r.pendingReadCount() != 0 {
		t.Fatal("concurrent reads did not complete")
	}

	if r.completedReads[0].RequestID != second.id ||
		r.completedReads[1].RequestID != first.id {
		t.Fatal("read identities were mixed")
	}
}

func TestReadAutoCompletionRejectedResponse(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})
	request := startAutomaticRead(t, r, "rejected")

	reply := readHeartbeatAck(r, request, 2)
	reply.Reject = true

	stepReadHeartbeatAck(t, r, reply)

	if r.completedReadCount() != 0 ||
		r.pendingReadCount() != 1 {
		t.Fatal("rejected response completed read")
	}
}

func TestReadAutoCompletionStaleResponse(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})
	request := startAutomaticRead(t, r, "stale")

	reply := readHeartbeatAck(r, request, 2)
	reply.Term--

	stepReadHeartbeatAck(t, r, reply)

	if r.completedReadCount() != 0 {
		t.Fatal("stale response completed read")
	}
}

func TestReadAutoCompletionUnknownRequest(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})
	request := startAutomaticRead(t, r, "unknown")

	reply := readHeartbeatAck(r, request, 2)
	reply.RequestID++

	stepReadHeartbeatAck(t, r, reply)

	if r.completedReadCount() != 0 ||
		r.pendingReadCount() != 1 {
		t.Fatal("unknown response completed read")
	}
}

func TestReadAutoCompletionUnknownPeer(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})
	request := startAutomaticRead(t, r, "unknown-peer")

	reply := readHeartbeatAck(r, request, 99)
	reply.Term++

	before := r.hard

	stepReadHeartbeatAck(t, r, reply)

	if r.hard != before ||
		r.volatile.Role != Leader ||
		r.completedReadCount() != 0 {
		t.Fatal("unknown peer influenced read state")
	}
}

func TestReadAutoCompletionHigherTerm(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})
	request := startAutomaticRead(t, r, "higher-term")

	reply := readHeartbeatAck(r, request, 2)
	reply.Term++

	stepReadHeartbeatAck(t, r, reply)

	if r.volatile.Role != Follower ||
		r.hard.Term != reply.Term {
		t.Fatal("valid higher-term response was ignored")
	}

	if r.completedReadCount() != 0 {
		t.Fatal("read completed after leadership loss")
	}
}

func TestReadAutoCompletionMalformedHigherTermResponse(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Message)
	}{
		{"index", func(m *Message) { m.Index = 1 }},
		{"log-term", func(m *Message) { m.LogTerm = 1 }},
		{"commit", func(m *Message) { m.Commit = 1 }},
		{"entries", func(m *Message) {
			m.Entries = []Entry{{Index: 1, Term: 1}}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReadAPILeader(t, []NodeID{2, 3})
			request := startAutomaticRead(t, r, "malformed")

			reply := readHeartbeatAck(r, request, 2)
			reply.Term++
			tc.edit(&reply)

			beforeHard := r.hard
			beforeVolatile := r.volatile

			stepReadHeartbeatAck(t, r, reply)

			if r.hard != beforeHard ||
				r.volatile != beforeVolatile ||
				r.completedReadCount() != 0 ||
				r.pendingReadCount() != 1 {
				t.Fatal("malformed response influenced state")
			}
		})
	}
}

func TestReadAutoCompletionMalformedHigherTermProbe(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Message)
	}{
		{"index", func(m *Message) { m.Index = 1 }},
		{"log-term", func(m *Message) { m.LogTerm = 1 }},
		{"commit", func(m *Message) { m.Commit = 1 }},
		{"reject", func(m *Message) { m.Reject = true }},
		{"entries", func(m *Message) {
			m.Entries = []Entry{{Index: 1, Term: 1}}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReadAPILeader(t, []NodeID{2, 3})

			msg := Message{
				Type:      MsgHeartbeat,
				From:      2,
				To:        r.id,
				Term:      r.hard.Term + 1,
				RequestID: 101,
			}

			tc.edit(&msg)

			beforeHard := r.hard
			beforeVolatile := r.volatile

			if err := r.Step(msg); err != nil {
				t.Fatal(err)
			}

			if r.hard != beforeHard ||
				r.volatile != beforeVolatile {
				t.Fatal("malformed probe changed leadership")
			}

			messages := r.Ready().Messages

			if len(messages) != 1 ||
				messages[0].Type != MsgHeartbeatResp ||
				!messages[0].Reject ||
				messages[0].RequestID != msg.RequestID ||
				messages[0].Term != beforeHard.Term {
				t.Fatal("expected rejected heartbeat response")
			}
		})
	}
}

func TestReadAutoCompletionActualFollower(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(
		t, leader, followers[2],
	)

	request := startAutomaticRead(
		t, leader, "actual-follower",
	)

	var probe Message
	found := false

	for _, msg := range leader.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID == request.id &&
			msg.To == 2 {
			probe = msg
			found = true
		}
	}

	if !found {
		t.Fatal("read probe missing")
	}

	leader.Advance()

	reply := deliverReadHeartbeatRequest(
		t, followers[2], probe,
	)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.completedReadCount() != 1 ||
		leader.pendingReadCount() != 0 {
		t.Fatal("actual follower did not confirm read")
	}
}

func TestReadAutoCompletionIsolatedLeader(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})
	request := startAutomaticRead(t, r, "isolated")

	for i := 0; i < 20; i++ {
		r.sendReadHeartbeats(request)
		r.Advance()
	}

	if r.completedReadCount() != 0 ||
		r.pendingReadCount() != 1 {
		t.Fatal("isolated leader completed read")
	}
}

func TestReadAutoCompletionNoDurableChanges(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	beforeHard := r.hard
	beforeVolatile := r.volatile
	beforeLog := cloneEntries(r.log)
	beforeSnapshot := r.SnapshotMetadata()
	beforeAppendID := r.nextRequestID

	for i := 0; i < 100; i++ {
		request := startAutomaticRead(t, r, "read-only")

		stepReadHeartbeatAck(
			t, r, readHeartbeatAck(r, request, 2),
		)
	}

	if r.completedReadCount() != 100 ||
		r.pendingReadCount() != 0 {
		t.Fatal("incorrect completed read count")
	}

	if r.hard != beforeHard ||
		r.volatile != beforeVolatile ||
		!reflect.DeepEqual(r.log, beforeLog) ||
		r.SnapshotMetadata() != beforeSnapshot ||
		r.nextRequestID != beforeAppendID {
		t.Fatal("automatic completion changed durable state")
	}

	rd := r.Ready()

	if rd.HardState != nil ||
		rd.TruncateFrom != 0 ||
		len(rd.Entries) != 0 ||
		len(rd.CommittedEntries) != 0 {
		t.Fatal("automatic completion generated durable work")
	}
}

func TestReadAutoCompletionManualCompatibility(t *testing.T) {
	r := newReadAPILeader(t, []NodeID{2, 3})

	request := registerReadCompletionRequest(
		t, r, "manual",
	)

	stepReadHeartbeatAck(
		t, r, readHeartbeatAck(r, request, 2),
	)

	if !r.hasReadQuorum(request) {
		t.Fatal("expected confirmed quorum")
	}

	if r.completedReadCount() != 0 ||
		r.pendingReadCount() != 1 {
		t.Fatal("manual request completed automatically")
	}

	if !r.completePendingRead(request) {
		t.Fatal("manual completion failed")
	}
}
