package raft

import (
	"reflect"
	"testing"
)

func newReadHeartbeatFollower() *Raft {
	r := NewRaft(2, []NodeID{1, 3}, 5)

	r.becomeFollower(5, 1)
	r.Advance()

	return r
}

func readHeartbeatMessage() Message {
	return Message{
		Type:      MsgHeartbeat,
		From:      1,
		To:        2,
		Term:      5,
		RequestID: 101,
	}
}

func requireReadHeartbeatReply(
	t *testing.T,
	r *Raft,
	request Message,
	rejected bool,
) Message {
	t.Helper()

	messages := r.Ready().Messages

	if len(messages) != 1 {
		t.Fatalf(
			"expected one response, got %d",
			len(messages),
		)
	}

	reply := messages[0]

	if reply.Type != MsgHeartbeatResp ||
		reply.From != request.To ||
		reply.To != request.From ||
		reply.RequestID != request.RequestID ||
		reply.Term != r.hard.Term ||
		reply.Reject != rejected {
		t.Fatalf("unexpected response: %+v", reply)
	}

	if reply.Index != 0 ||
		reply.LogTerm != 0 ||
		reply.Commit != 0 ||
		len(reply.Entries) != 0 {
		t.Fatal("unexpected replication payload")
	}

	return reply
}

func TestReadHeartbeatReceiverAcknowledgment(t *testing.T) {
	r := newReadHeartbeatFollower()
	msg := readHeartbeatMessage()

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	requireReadHeartbeatReply(t, r, msg, false)

	if r.Status().Role != Follower ||
		r.Status().LeaderID != msg.From {
		t.Fatal("incorrect follower state")
	}
}

func TestReadHeartbeatReceiverStaleTerm(t *testing.T) {
	r := newReadHeartbeatFollower()

	r.volatile.ElectionElapsed = 3

	msg := readHeartbeatMessage()
	msg.Term = 4

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	requireReadHeartbeatReply(t, r, msg, true)

	if r.hard.Term != 5 {
		t.Fatal("stale heartbeat changed term")
	}

	if r.volatile.ElectionElapsed != 3 {
		t.Fatal("stale heartbeat reset election timer")
	}
}

func TestReadHeartbeatReceiverHigherTerm(t *testing.T) {
	r := newReadHeartbeatFollower()

	msg := readHeartbeatMessage()
	msg.Term = 6

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	requireReadHeartbeatReply(t, r, msg, false)

	if r.hard.Term != 6 {
		t.Fatal("higher term was not adopted")
	}

	if r.Status().LeaderID != 1 {
		t.Fatal("leader ID was not updated")
	}

	if r.Ready().HardState == nil {
		t.Fatal("term change was not exposed for persistence")
	}
}

func TestReadHeartbeatReceiverUnknownPeer(t *testing.T) {
	r := newReadHeartbeatFollower()

	before := r.Status()

	msg := readHeartbeatMessage()
	msg.From = 99
	msg.Term = 100

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	if len(r.Ready().Messages) != 0 {
		t.Fatal("unknown peer received acknowledgment")
	}

	if r.Status() != before {
		t.Fatal("unknown peer changed Raft status")
	}
}

func TestReadHeartbeatReceiverInvalidSender(t *testing.T) {
	for _, sender := range []NodeID{0, 2} {
		r := newReadHeartbeatFollower()

		before := r.Status()

		msg := readHeartbeatMessage()
		msg.From = sender
		msg.Term = 100

		if err := r.Step(msg); err != nil {
			t.Fatal(err)
		}

		if len(r.Ready().Messages) != 0 {
			t.Fatal("invalid sender received acknowledgment")
		}

		if r.Status() != before {
			t.Fatal("invalid sender changed Raft status")
		}
	}
}

func TestReadHeartbeatReceiverWrongDestination(t *testing.T) {
	r := newReadHeartbeatFollower()

	msg := readHeartbeatMessage()
	msg.To = 3

	if err := r.Step(msg); err == nil {
		t.Fatal("wrong destination was accepted")
	}

	if len(r.Ready().Messages) != 0 {
		t.Fatal("wrong destination generated response")
	}
}

func TestReadHeartbeatReceiverResetsElectionTimer(t *testing.T) {
	r := newReadHeartbeatFollower()

	r.volatile.ElectionElapsed = 4

	msg := readHeartbeatMessage()

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	requireReadHeartbeatReply(t, r, msg, false)

	if r.volatile.ElectionElapsed != 0 {
		t.Fatal("election timer was not reset")
	}
}

func TestReadHeartbeatReceiverPreservesLog(t *testing.T) {
	r := newReadHeartbeatFollower()

	r.appendEntry([]byte("existing-entry"), EntryNormal)

	r.hard.Commit = 1
	r.volatile.CommitIndex = 1

	r.Advance()

	beforeLog := cloneEntries(r.log)
	beforeHard := r.hard
	beforeApplied := r.volatile.AppliedIndex

	msg := readHeartbeatMessage()

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	requireReadHeartbeatReply(t, r, msg, false)

	if !reflect.DeepEqual(beforeLog, r.log) {
		t.Fatal("read heartbeat modified log")
	}

	if r.hard != beforeHard {
		t.Fatal("read heartbeat modified HardState")
	}

	if r.volatile.AppliedIndex != beforeApplied {
		t.Fatal("read heartbeat modified applied index")
	}

	ready := r.Ready()

	if len(ready.Entries) != 0 ||
		len(ready.CommittedEntries) != 0 ||
		ready.HardState != nil ||
		ready.TruncateFrom != 0 {
		t.Fatal("read heartbeat generated durable work")
	}
}

func TestReadHeartbeatReceiverMalformedPayload(t *testing.T) {
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
					{Index: 1, Term: 5},
				}
			},
		},
		{
			name: "reject-flag",
			mutate: func(m *Message) {
				m.Reject = true
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReadHeartbeatFollower()

			r.volatile.ElectionElapsed = 3

			before := r.hard

			msg := readHeartbeatMessage()
			tc.mutate(&msg)

			if err := r.Step(msg); err != nil {
				t.Fatal(err)
			}

			requireReadHeartbeatReply(t, r, msg, true)

			if r.hard != before ||
				len(r.log) != 0 {
				t.Fatal("malformed heartbeat changed durable state")
			}

			if r.volatile.ElectionElapsed != 3 {
				t.Fatal("malformed heartbeat reset election timer")
			}
		})
	}
}

func TestReadHeartbeatReceiverOrdinaryCompatibility(t *testing.T) {
	r := newReadHeartbeatFollower()

	r.appendEntry(nil, EntryNoop)
	r.Advance()

	msg := Message{
		Type:    MsgHeartbeat,
		From:    1,
		To:      2,
		Term:    5,
		Index:   1,
		LogTerm: 5,
		Commit:  1,
	}

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	if r.hard.Commit != 1 {
		t.Fatal("ordinary heartbeat did not advance commit")
	}

	if len(r.Ready().Messages) != 0 {
		t.Fatal("ordinary heartbeat generated read acknowledgment")
	}
}

func TestReadHeartbeatReceiverDuplicateRequest(t *testing.T) {
	r := newReadHeartbeatFollower()

	msg := readHeartbeatMessage()

	for i := 0; i < 10; i++ {
		if err := r.Step(msg); err != nil {
			t.Fatal(err)
		}

		requireReadHeartbeatReply(t, r, msg, false)

		r.Advance()
	}

	if r.lastIndex() != 0 ||
		r.hard.Commit != 0 {
		t.Fatal("duplicate read heartbeat modified log")
	}
}

func TestReadHeartbeatReceiverCandidateStepsDown(t *testing.T) {
	r := newReadHeartbeatFollower()

	r.startElection()
	r.Advance()

	msg := readHeartbeatMessage()
	msg.Term = r.hard.Term

	if err := r.Step(msg); err != nil {
		t.Fatal(err)
	}

	requireReadHeartbeatReply(t, r, msg, false)

	if r.Status().Role != Follower {
		t.Fatal("candidate did not step down")
	}

	if r.Status().LeaderID != msg.From {
		t.Fatal("candidate did not recognize leader")
	}
}
