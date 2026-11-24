package raft

import (
	"fmt"
	"testing"
)

func TestReadQuorumClusterSizes(t *testing.T) {
	for _, size := range []int{1, 2, 3, 4, 5, 7} {
		t.Run(fmt.Sprintf("nodes-%d", size), func(t *testing.T) {
			var peers []NodeID

			for i := 2; i <= size; i++ {
				peers = append(peers, NodeID(i))
			}

			r := newReadHeartbeatLeader(t, peers)
			request := registerReadHeartbeatRequest(t, r)

			r.sendReadHeartbeats(request)

			required := size/2 + 1

			if r.hasReadQuorum(request) != (required == 1) {
				t.Fatal("incorrect initial quorum state")
			}

			for i, peer := range peers {
				stepReadHeartbeatAck(
					t,
					r,
					readHeartbeatAck(r, request, peer),
				)

				expected := i+2 >= required

				if r.hasReadQuorum(request) != expected {
					t.Fatalf(
						"incorrect quorum with %d voters",
						i+2,
					)
				}
			}
		})
	}
}

func TestReadQuorumDuplicateAcknowledgments(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3, 4, 5},
	)

	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	for i := 0; i < 100; i++ {
		stepReadHeartbeatAck(
			t,
			r,
			readHeartbeatAck(r, request, 2),
		)
	}

	if r.hasReadQuorum(request) {
		t.Fatal("duplicate acknowledgments satisfied quorum")
	}

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, request, 3),
	)

	if !r.hasReadQuorum(request) {
		t.Fatal("distinct voter quorum was not detected")
	}
}

func TestReadQuorumUnknownVoters(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3, 4, 5},
	)

	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	request.acks[0] = struct{}{}
	request.acks[99] = struct{}{}
	request.acks[100] = struct{}{}

	if r.hasReadQuorum(request) {
		t.Fatal("unknown voters satisfied quorum")
	}

	for _, peer := range []NodeID{2, 3} {
		stepReadHeartbeatAck(
			t,
			r,
			readHeartbeatAck(r, request, peer),
		)
	}

	if !r.hasReadQuorum(request) {
		t.Fatal("configured voters did not satisfy quorum")
	}
}

func TestReadQuorumUnstartedRequest(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, request, 2),
	)

	if len(request.acks) != 1 {
		t.Fatal("unsent read request accepted acknowledgment")
	}

	if r.hasReadQuorum(request) {
		t.Fatal("unstarted request satisfied quorum")
	}

	r.sendReadHeartbeats(request)

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, request, 2),
	)

	if !r.hasReadQuorum(request) {
		t.Fatal("started request failed quorum confirmation")
	}
}

func TestReadQuorumSingleNodeRequiresStart(t *testing.T) {
	r := newReadHeartbeatLeader(t, nil)
	request := registerReadHeartbeatRequest(t, r)

	if r.hasReadQuorum(request) {
		t.Fatal("unstarted single-node request confirmed")
	}

	r.sendReadHeartbeats(request)

	if !r.hasReadQuorum(request) {
		t.Fatal("single-node quorum not confirmed")
	}

	if len(r.Ready().Messages) != 0 {
		t.Fatal("single-node quorum generated network traffic")
	}
}

func TestReadQuorumRemovedRequest(t *testing.T) {
	r := newReadHeartbeatLeader(t, nil)
	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	if !r.hasReadQuorum(request) {
		t.Fatal("expected initial quorum")
	}

	r.removePendingRead(request.id)

	if r.hasReadQuorum(request) {
		t.Fatal("removed request retained quorum")
	}
}

func TestReadQuorumStaleTerm(t *testing.T) {
	r := newReadHeartbeatLeader(t, nil)
	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	request.term++

	if r.hasReadQuorum(request) {
		t.Fatal("different-term request retained quorum")
	}
}

func TestReadQuorumLeadershipLoss(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, request, 2),
	)

	if !r.hasReadQuorum(request) {
		t.Fatal("expected quorum before leadership loss")
	}

	r.becomeFollower(r.hard.Term+1, 2)

	if r.hasReadQuorum(request) {
		t.Fatal("former leader retained quorum")
	}
}

func TestReadQuorumConcurrentRequests(t *testing.T) {
	r := newReadHeartbeatLeader(
		t,
		[]NodeID{2, 3, 4, 5},
	)

	first := registerReadHeartbeatRequest(t, r)
	second := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(first)
	r.sendReadHeartbeats(second)

	for _, peer := range []NodeID{2, 3} {
		stepReadHeartbeatAck(
			t,
			r,
			readHeartbeatAck(r, first, peer),
		)
	}

	if !r.hasReadQuorum(first) {
		t.Fatal("first request failed quorum")
	}

	if r.hasReadQuorum(second) {
		t.Fatal("second request inherited quorum")
	}

	for _, peer := range []NodeID{4, 5} {
		stepReadHeartbeatAck(
			t,
			r,
			readHeartbeatAck(r, second, peer),
		)
	}

	if !r.hasReadQuorum(second) {
		t.Fatal("second request failed independent quorum")
	}
}

func TestReadQuorumInvalidRequest(t *testing.T) {
	r := newReadHeartbeatLeader(t, nil)

	if r.hasReadQuorum(nil) {
		t.Fatal("nil request satisfied quorum")
	}

	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	copied := *request

	if r.hasReadQuorum(&copied) {
		t.Fatal("unregistered request object satisfied quorum")
	}
}

func TestReadQuorumNoDurableChanges(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)

	beforeHard := r.hard
	beforeIndex := r.lastIndex()
	beforeApplied := r.volatile.AppliedIndex

	r.sendReadHeartbeats(request)

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, request, 2),
	)

	if !r.hasReadQuorum(request) {
		t.Fatal("expected quorum")
	}

	if r.hard != beforeHard ||
		r.lastIndex() != beforeIndex ||
		r.volatile.AppliedIndex != beforeApplied {
		t.Fatal("quorum detection changed Raft state")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("quorum detection prematurely completed read")
	}

	ready := r.Ready()

	if len(ready.Entries) != 0 ||
		ready.HardState != nil ||
		ready.TruncateFrom != 0 {
		t.Fatal("quorum detection generated durable work")
	}
}

func TestReadQuorumZeroIDHigherTermResponse(t *testing.T) {
	r := newReadHeartbeatLeader(t, []NodeID{2, 3})
	request := registerReadHeartbeatRequest(t, r)

	r.sendReadHeartbeats(request)

	before := r.Status()

	msg := readHeartbeatAck(r, request, 99)
	msg.RequestID = 0
	msg.Term += 100

	stepReadHeartbeatAck(t, r, msg)

	if r.Status() != before {
		t.Fatal("invalid response changed leadership state")
	}

	if len(request.acks) != 1 {
		t.Fatal("invalid response counted toward quorum")
	}
}
