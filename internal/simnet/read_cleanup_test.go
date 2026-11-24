package simnet

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSimReadCleanupObsolete(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	submitSimRead(t, c, leader, "obsolete")
	deliverSimRead(t, c)

	if len(c.readStates) != 1 {
		t.Fatal("expected collected confirmation")
	}

	var peer raft.NodeID

	for _, id := range c.IDs {
		if id != leader {
			peer = id
			break
		}
	}

	node := c.Nodes[leader]

	if err := node.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: peer,
		To:   leader,
		Term: node.Status().Term + 1,
	}); err != nil {
		t.Fatal(err)
	}

	deliverSimRead(t, c)

	if len(c.readStates) != 0 {
		t.Fatal("obsolete confirmation retained")
	}

	if c.readStates != nil {
		t.Fatal("empty queue backing storage retained")
	}

	if len(c.DrainReadStates()) != 0 {
		t.Fatal("obsolete confirmation exposed")
	}
}

func TestSimReadCleanupCurrent(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	for _, ctx := range []string{"first", "second"} {
		submitSimRead(t, c, leader, ctx)
	}

	deliverSimRead(t, c)

	for i := 0; i < 100; i++ {
		c.pruneObsoleteReadStates()
	}

	states := c.DrainReadStates()

	if len(states) != 2 {
		t.Fatal("current confirmations lost")
	}

	if string(states[0].State.Context) != "first" ||
		string(states[1].State.Context) != "second" {
		t.Fatal("confirmation ordering changed")
	}
}

func TestSimReadCleanupDeferred(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	submitSimRead(t, c, leader, "deferred")
	deliverSimRead(t, c)

	for i := 0; i < 100; i++ {
		c.pruneObsoleteReadStates()
	}

	if len(c.readStates) != 1 {
		t.Fatal("deferred confirmation discarded")
	}

	if len(c.ReadStates()) != 0 {
		t.Fatal("application barrier bypassed")
	}

	resumeSimApplication(t, c, leader)

	requireSimReadContext(
		t, c, leader, "deferred",
	)

	if len(c.readStates) != 0 {
		t.Fatal("consumed confirmation retained")
	}
}

func TestSimReadCleanupCapacity(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "original")
	deliverSimRead(t, c)

	if len(c.readStates) != 1 {
		t.Fatal("expected initial confirmation")
	}

	// Populate the queue with synthetic obsolete
	// confirmations to exercise capacity reclamation.
	obsolete := c.readStates[0]
	obsolete.State.Term = 0

	for len(c.readStates) < maxCollectedReadStates {
		c.readStates = append(c.readStates, obsolete)
	}

	submitSimRead(t, c, leader, "replacement")

	deliverSimRead(t, c)

	states := c.DrainReadStates()

	if len(states) != 2 {
		t.Fatalf(
			"expected two valid confirmations, got %d",
			len(states),
		)
	}

	if string(states[0].State.Context) != "original" ||
		string(states[1].State.Context) != "replacement" {
		t.Fatal("incorrect confirmations after cleanup")
	}

	if len(c.Nodes[leader].Ready().ReadStates) != 0 {
		t.Fatal("Raft confirmation was not acknowledged")
	}

	if len(c.readStates) != 0 {
		t.Fatal("queue was not drained")
	}
}
