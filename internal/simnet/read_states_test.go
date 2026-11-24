package simnet

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func submitSimRead(
	t *testing.T,
	c *Cluster,
	leader raft.NodeID,
	ctx string,
) {
	t.Helper()

	if err := c.ReadIndex(
		leader, []byte(ctx),
	); err != nil {
		t.Fatal(err)
	}
}

func deliverSimRead(t *testing.T, c *Cluster) {
	t.Helper()

	if err := c.Deliver(); err != nil {
		t.Fatal(err)
	}
}

func TestSimReadCollectionEmpty(t *testing.T) {
	c, _ := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	if len(c.ReadStates()) != 0 {
		t.Fatal("unexpected confirmation")
	}

	if len(c.DrainReadStates()) != 0 {
		t.Fatal("unexpected drained confirmation")
	}
}

func TestSimReadCollectionSingleNode(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "single")
	deliverSimRead(t, c)

	states := c.ReadStates()

	if len(states) != 1 {
		t.Fatalf("expected one confirmation, got %d", len(states))
	}

	state := states[0]
	status := c.Nodes[leader].Status()

	if state.NodeID != leader ||
		state.State.RequestID == 0 ||
		state.State.Term != status.Term ||
		state.State.Index != status.Commit ||
		string(state.State.Context) != "single" {
		t.Fatalf("invalid confirmation: %+v", state)
	}
}

func TestSimReadCollectionThreeNodes(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	submitSimRead(t, c, leader, "three")

	if len(c.ReadStates()) != 0 {
		t.Fatal("read confirmed before delivery")
	}

	deliverSimRead(t, c)

	states := c.ReadStates()

	if len(states) != 1 ||
		states[0].NodeID != leader ||
		string(states[0].State.Context) != "three" {
		t.Fatal("incorrect three-node confirmation")
	}
}

func TestSimReadCollectionFiveNodes(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3, 4, 5},
	)

	submitSimRead(t, c, leader, "five")
	deliverSimRead(t, c)

	states := c.ReadStates()

	if len(states) != 1 ||
		states[0].NodeID != leader {
		t.Fatal("incorrect five-node confirmation")
	}
}

func TestSimReadCollectionAcknowledgment(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "ack")

	if len(c.Nodes[leader].Ready().ReadStates) != 1 {
		t.Fatal("expected queued Raft confirmation")
	}

	deliverSimRead(t, c)

	if len(c.Nodes[leader].Ready().ReadStates) != 0 {
		t.Fatal("Raft confirmation was not acknowledged")
	}

	if len(c.ReadStates()) != 1 {
		t.Fatal("simulator lost confirmation")
	}
}

func TestSimReadCollectionNoDuplicateDelivery(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	submitSimRead(t, c, leader, "once")
	deliverSimRead(t, c)

	for i := 0; i < 10; i++ {
		c.Tick()
		deliverSimRead(t, c)
	}

	if len(c.ReadStates()) != 1 {
		t.Fatal("confirmation delivered more than once")
	}
}

func TestSimReadCollectionMultipleRequests(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	for _, ctx := range []string{"a", "b", "c"} {
		submitSimRead(t, c, leader, ctx)
	}

	deliverSimRead(t, c)

	states := c.ReadStates()

	if len(states) != 3 {
		t.Fatalf("expected 3 confirmations, got %d", len(states))
	}

	ids := make(map[uint64]bool)
	contexts := make(map[string]bool)

	for _, item := range states {
		if item.NodeID != leader {
			t.Fatal("incorrect source node")
		}

		if ids[item.State.RequestID] {
			t.Fatal("duplicate request ID")
		}

		ids[item.State.RequestID] = true
		contexts[string(item.State.Context)] = true
	}

	for _, ctx := range []string{"a", "b", "c"} {
		if !contexts[ctx] {
			t.Fatalf("missing context %q", ctx)
		}
	}
}

func TestSimReadCollectionDuplicateContexts(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "same")
	submitSimRead(t, c, leader, "same")
	deliverSimRead(t, c)

	states := c.ReadStates()

	if len(states) != 2 {
		t.Fatal("duplicate contexts were incorrectly merged")
	}

	if states[0].State.RequestID ==
		states[1].State.RequestID {
		t.Fatal("duplicate request identity")
	}
}

func TestSimReadCollectionContextIsolation(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "original")
	deliverSimRead(t, c)

	first := c.ReadStates()
	first[0].State.Context[0] = 'X'

	second := c.ReadStates()

	if string(second[0].State.Context) != "original" {
		t.Fatal("returned context mutated simulator state")
	}
}

func TestSimReadCollectionStateIsolation(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "isolation")
	deliverSimRead(t, c)

	first := c.ReadStates()

	first[0].NodeID = 99
	first[0].State.RequestID = 0
	first[0].State.Term = 0
	first[0].State.Index = 0

	second := c.ReadStates()

	if second[0].NodeID != leader ||
		second[0].State.RequestID == 0 ||
		second[0].State.Term == 0 ||
		second[0].State.Index == 0 {
		t.Fatal("returned state mutated simulator queue")
	}
}

func TestSimReadCollectionDrain(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "drain")
	deliverSimRead(t, c)

	states := c.DrainReadStates()

	if len(states) != 1 {
		t.Fatal("expected one drained confirmation")
	}

	if len(c.ReadStates()) != 0 ||
		len(c.DrainReadStates()) != 0 {
		t.Fatal("drain did not clear queue")
	}

	if string(states[0].State.Context) != "drain" {
		t.Fatal("drained context was corrupted")
	}
}

func TestSimReadCollectionAfterDrain(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "first")
	deliverSimRead(t, c)

	first := c.DrainReadStates()

	submitSimRead(t, c, leader, "second")
	deliverSimRead(t, c)

	second := c.DrainReadStates()

	if len(first) != 1 || len(second) != 1 {
		t.Fatal("missing confirmation")
	}

	if second[0].State.RequestID <=
		first[0].State.RequestID {
		t.Fatal("request IDs did not increase")
	}

	if string(second[0].State.Context) != "second" {
		t.Fatal("incorrect second confirmation")
	}
}

func TestSimReadCollectionPendingNotExposed(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.DropMessage = func(msg raft.Message) bool {
		return msg.Type == raft.MsgHeartbeatResp &&
			msg.RequestID != 0
	}

	submitSimRead(t, c, leader, "pending")
	deliverSimRead(t, c)

	if len(c.ReadStates()) != 0 {
		t.Fatal("unconfirmed read was exposed")
	}
}

func TestSimReadCollectionLeadershipInvalidation(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	submitSimRead(t, c, leader, "obsolete")
	deliverSimRead(t, c)

	if len(c.ReadStates()) != 1 {
		t.Fatal("expected initial confirmation")
	}

	node := c.Nodes[leader]
	status := node.Status()

	var peer raft.NodeID

	for _, id := range c.IDs {
		if id != leader {
			peer = id
			break
		}
	}

	err := node.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: peer,
		To:   leader,
		Term: status.Term + 1,
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(c.ReadStates()) != 0 ||
		len(c.DrainReadStates()) != 0 {
		t.Fatal("obsolete confirmation was exposed")
	}
}

func TestSimReadCollectionCapacity(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	for i := 0; i < maxCollectedReadStates; i++ {
		submitSimRead(t, c, leader, "capacity")
		deliverSimRead(t, c)
	}

	submitSimRead(t, c, leader, "overflow")

	if err := c.Deliver(); err == nil {
		t.Fatal("expected simulator capacity error")
	}

	if len(c.Nodes[leader].Ready().ReadStates) != 1 {
		t.Fatal("overflow confirmation was lost")
	}

	if len(c.DrainReadStates()) != maxCollectedReadStates {
		t.Fatal("incorrect queue size")
	}

	deliverSimRead(t, c)

	states := c.DrainReadStates()

	if len(states) != 1 ||
		string(states[0].State.Context) != "overflow" {
		t.Fatal("failed to recover retained confirmation")
	}
}

func TestSimReadCollectionNoDurableChanges(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	node := c.Nodes[leader]
	before := node.Status()

	for i := 0; i < 100; i++ {
		submitSimRead(t, c, leader, "read-only")
	}

	deliverSimRead(t, c)

	if len(c.DrainReadStates()) != 100 {
		t.Fatal("missing read confirmations")
	}

	if before != node.Status() {
		t.Fatal("reads changed Raft status")
	}

	ready := node.Ready()

	if ready.HardState != nil ||
		len(ready.Entries) != 0 ||
		len(ready.CommittedEntries) != 0 ||
		ready.TruncateFrom != 0 {
		t.Fatal("reads generated durable work")
	}

	faultTestSafety(t, c)
}
