package raft

import (
	"reflect"
	"testing"
)

func newReadHeartbeatIntegrationCluster(
	t *testing.T,
	size int,
) (*Raft, map[NodeID]*Raft) {
	t.Helper()

	var peers []NodeID

	for i := 2; i <= size; i++ {
		peers = append(peers, NodeID(i))
	}

	leader := newReadHeartbeatLeader(t, peers)

	followers := make(map[NodeID]*Raft)

	for _, id := range peers {
		var remotePeers []NodeID

		for i := 1; i <= size; i++ {
			if NodeID(i) != id {
				remotePeers = append(
					remotePeers,
					NodeID(i),
				)
			}
		}

		follower := NewRaft(id, remotePeers, 5)

		follower.becomeFollower(
			leader.hard.Term,
			leader.id,
		)

		follower.Advance()

		followers[id] = follower
	}

	return leader, followers
}

func collectReadHeartbeatRequests(
	t *testing.T,
	leader *Raft,
	request *pendingRead,
) []Message {
	t.Helper()

	leader.sendReadHeartbeats(request)

	var messages []Message

	for _, msg := range leader.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID == request.id {
			messages = append(messages, msg)
		}
	}

	leader.Advance()

	return messages
}

func deliverReadHeartbeatRequest(
	t *testing.T,
	follower *Raft,
	msg Message,
) Message {
	t.Helper()

	if err := follower.Step(msg); err != nil {
		t.Fatal(err)
	}

	messages := follower.Ready().Messages

	if len(messages) != 1 {
		t.Fatalf(
			"expected one response, got %d",
			len(messages),
		)
	}

	reply := messages[0]

	if reply.Type != MsgHeartbeatResp ||
		reply.RequestID != msg.RequestID ||
		reply.From != msg.To ||
		reply.To != msg.From {
		t.Fatalf("incorrect response: %+v", reply)
	}

	follower.Advance()

	return reply
}

func TestReadHeartbeatIntegrationThreeNodes(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	request := registerReadHeartbeatRequest(t, leader)

	messages := collectReadHeartbeatRequests(
		t,
		leader,
		request,
	)

	if len(messages) != 2 {
		t.Fatal("expected two read heartbeats")
	}

	if leader.hasReadQuorum(request) {
		t.Fatal("quorum confirmed before delivery")
	}

	reply := deliverReadHeartbeatRequest(
		t,
		followers[messages[0].To],
		messages[0],
	)

	if reply.Reject {
		t.Fatal("valid heartbeat rejected")
	}

	stepReadHeartbeatAck(t, leader, reply)

	if !leader.hasReadQuorum(request) {
		t.Fatal("three-node quorum not confirmed")
	}

	if leader.pendingReadCount() != 1 {
		t.Fatal("read completed prematurely")
	}
}

func TestReadHeartbeatIntegrationFiveNodes(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 5)

	request := registerReadHeartbeatRequest(t, leader)

	messages := collectReadHeartbeatRequests(
		t,
		leader,
		request,
	)

	if len(messages) != 4 {
		t.Fatal("expected four read heartbeats")
	}

	for i, msg := range messages {
		reply := deliverReadHeartbeatRequest(
			t,
			followers[msg.To],
			msg,
		)

		stepReadHeartbeatAck(t, leader, reply)

		expected := i >= 1

		if leader.hasReadQuorum(request) != expected {
			t.Fatalf(
				"incorrect quorum after %d responses",
				i+1,
			)
		}
	}
}

func TestReadHeartbeatIntegrationLostHeartbeatRetry(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	request := registerReadHeartbeatRequest(t, leader)

	first := collectReadHeartbeatRequests(
		t,
		leader,
		request,
	)

	if len(first) != 2 {
		t.Fatal("expected initial heartbeat round")
	}

	// Simulate dropping the entire first round.
	if leader.hasReadQuorum(request) {
		t.Fatal("dropped messages satisfied quorum")
	}

	retry := collectReadHeartbeatRequests(
		t,
		leader,
		request,
	)

	if len(retry) != 2 {
		t.Fatal("expected retransmission")
	}

	for _, msg := range retry {
		if msg.RequestID != request.id {
			t.Fatal("retry changed request ID")
		}
	}

	reply := deliverReadHeartbeatRequest(
		t,
		followers[retry[0].To],
		retry[0],
	)

	stepReadHeartbeatAck(t, leader, reply)

	if !leader.hasReadQuorum(request) {
		t.Fatal("retry failed quorum confirmation")
	}

	if len(collectReadHeartbeatRequests(
		t,
		leader,
		request,
	)) != 0 {
		t.Fatal("confirmed request generated more heartbeats")
	}
}

func TestReadHeartbeatIntegrationDuplicateDelivery(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 5)

	request := registerReadHeartbeatRequest(t, leader)

	messages := collectReadHeartbeatRequests(
		t,
		leader,
		request,
	)

	reply := deliverReadHeartbeatRequest(
		t,
		followers[messages[0].To],
		messages[0],
	)

	for i := 0; i < 100; i++ {
		stepReadHeartbeatAck(t, leader, reply)
	}

	if leader.hasReadQuorum(request) {
		t.Fatal("duplicate responses satisfied quorum")
	}

	if len(request.acks) != 2 {
		t.Fatal("duplicate responses inflated voter count")
	}

	reply = deliverReadHeartbeatRequest(
		t,
		followers[messages[1].To],
		messages[1],
	)

	stepReadHeartbeatAck(t, leader, reply)

	if !leader.hasReadQuorum(request) {
		t.Fatal("distinct responses failed quorum")
	}
}

func TestReadHeartbeatIntegrationDelayedResponses(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	first := registerReadHeartbeatRequest(t, leader)
	second := registerReadHeartbeatRequest(t, leader)

	firstMessages := collectReadHeartbeatRequests(
		t,
		leader,
		first,
	)

	secondMessages := collectReadHeartbeatRequests(
		t,
		leader,
		second,
	)

	firstReply := deliverReadHeartbeatRequest(
		t,
		followers[firstMessages[0].To],
		firstMessages[0],
	)

	secondReply := deliverReadHeartbeatRequest(
		t,
		followers[secondMessages[1].To],
		secondMessages[1],
	)

	// Deliver the newer response first.
	stepReadHeartbeatAck(t, leader, secondReply)

	if !leader.hasReadQuorum(second) {
		t.Fatal("second request failed quorum")
	}

	if leader.hasReadQuorum(first) {
		t.Fatal("first request inherited another response")
	}

	stepReadHeartbeatAck(t, leader, firstReply)

	if !leader.hasReadQuorum(first) {
		t.Fatal("delayed first response was not correlated")
	}
}

func TestReadHeartbeatIntegrationIsolatedLeader(t *testing.T) {
	leader, _ :=
		newReadHeartbeatIntegrationCluster(t, 3)

	request := registerReadHeartbeatRequest(t, leader)

	for i := 0; i < 20; i++ {
		messages := collectReadHeartbeatRequests(
			t,
			leader,
			request,
		)

		if len(messages) != 2 {
			t.Fatal("expected retry to both followers")
		}

		// No messages are delivered.
		if leader.hasReadQuorum(request) {
			t.Fatal("isolated leader confirmed quorum")
		}
	}

	if len(request.acks) != 1 {
		t.Fatal("isolated leader recorded remote acknowledgment")
	}
}

func TestReadHeartbeatIntegrationHigherTerm(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	request := registerReadHeartbeatRequest(t, leader)

	messages := collectReadHeartbeatRequests(
		t,
		leader,
		request,
	)

	follower := followers[messages[0].To]

	higherTerm := leader.hard.Term + 1

	follower.becomeFollower(higherTerm, 0)
	follower.Advance()

	reply := deliverReadHeartbeatRequest(
		t,
		follower,
		messages[0],
	)

	if !reply.Reject ||
		reply.Term != higherTerm {
		t.Fatal("stale heartbeat was not correctly rejected")
	}

	stepReadHeartbeatAck(t, leader, reply)

	if leader.Status().Role != Follower {
		t.Fatal("stale leader did not step down")
	}

	if leader.Status().Term != higherTerm {
		t.Fatal("higher term was not adopted")
	}

	if leader.hasReadQuorum(request) {
		t.Fatal("former leader retained quorum")
	}
}

func TestReadHeartbeatIntegrationRemovedRequest(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	request := registerReadHeartbeatRequest(t, leader)

	messages := collectReadHeartbeatRequests(
		t,
		leader,
		request,
	)

	reply := deliverReadHeartbeatRequest(
		t,
		followers[messages[0].To],
		messages[0],
	)

	leader.removePendingRead(request.id)

	stepReadHeartbeatAck(t, leader, reply)

	if leader.hasReadQuorum(request) {
		t.Fatal("removed request regained quorum")
	}

	if leader.pendingReadCount() != 0 {
		t.Fatal("removed request was recreated")
	}

	if len(request.acks) != 1 {
		t.Fatal("removed request recorded acknowledgment")
	}
}

func TestReadHeartbeatIntegrationNoLogChanges(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	nodes := map[NodeID]*Raft{
		leader.id: leader,
	}

	for id, follower := range followers {
		nodes[id] = follower
	}

	beforeLogs := make(map[NodeID][]Entry)
	beforeHard := make(map[NodeID]HardState)

	for id, node := range nodes {
		beforeLogs[id] = cloneEntries(node.log)
		beforeHard[id] = node.hard
	}

	for i := 0; i < 100; i++ {
		request := registerReadHeartbeatRequest(t, leader)

		messages := collectReadHeartbeatRequests(
			t,
			leader,
			request,
		)

		for _, msg := range messages {
			reply := deliverReadHeartbeatRequest(
				t,
				followers[msg.To],
				msg,
			)

			stepReadHeartbeatAck(t, leader, reply)
		}

		if !leader.hasReadQuorum(request) {
			t.Fatal("read confirmation failed")
		}

		leader.removePendingRead(request.id)
	}

	for id, node := range nodes {
		if !reflect.DeepEqual(
			beforeLogs[id],
			node.log,
		) {
			t.Fatalf("node %d log changed", id)
		}

		if beforeHard[id] != node.hard {
			t.Fatalf("node %d HardState changed", id)
		}

		ready := node.Ready()

		if len(ready.Entries) != 0 ||
			len(ready.CommittedEntries) != 0 ||
			ready.HardState != nil ||
			ready.TruncateFrom != 0 {
			t.Fatalf("node %d generated durable work", id)
		}
	}

	if leader.pendingReadCount() != 0 {
		t.Fatal("pending read requests leaked")
	}
}
