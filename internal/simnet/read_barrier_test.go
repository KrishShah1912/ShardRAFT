package simnet

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func requireSimAppliedIndex(
	t *testing.T,
	c *Cluster,
	id raft.NodeID,
) uint64 {
	t.Helper()

	index, err := c.AppliedIndex(id)

	if err != nil {
		t.Fatal(err)
	}

	return index
}

func pauseSimApplication(
	t *testing.T,
	c *Cluster,
	id raft.NodeID,
) {
	t.Helper()

	if err := c.PauseApplication(id); err != nil {
		t.Fatal(err)
	}
}

func resumeSimApplication(
	t *testing.T,
	c *Cluster,
	id raft.NodeID,
) {
	t.Helper()

	if err := c.ResumeApplication(id); err != nil {
		t.Fatal(err)
	}
}

func proposeSimBarrierEntry(
	t *testing.T,
	c *Cluster,
	id raft.NodeID,
) {
	t.Helper()

	if err := c.Nodes[id].Propose(
		[]byte("barrier-command"),
	); err != nil {
		t.Fatal(err)
	}

	deliverSimRead(t, c)
}

func TestSimReadBarrierDefaultApplication(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	applied := requireSimAppliedIndex(t, c, leader)

	if applied == 0 ||
		applied != c.Nodes[leader].Status().Commit {
		t.Fatal("default application did not follow commit")
	}
}

func TestSimReadBarrierUnknownNode(t *testing.T) {
	c := NewCluster([]raft.NodeID{1})

	if _, err := c.AppliedIndex(99); err == nil {
		t.Fatal("unknown applied-index target accepted")
	}

	if err := c.PauseApplication(99); err == nil {
		t.Fatal("unknown pause target accepted")
	}

	if err := c.ResumeApplication(99); err == nil {
		t.Fatal("unknown resume target accepted")
	}
}

func TestSimReadBarrierPausedApplication(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	before := requireSimAppliedIndex(t, c, leader)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	if requireSimAppliedIndex(t, c, leader) != before {
		t.Fatal("paused state machine advanced")
	}

	if c.Nodes[leader].Status().Commit <= before {
		t.Fatal("Raft commit did not advance")
	}
}

func TestSimReadBarrierBehind(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	submitSimRead(t, c, leader, "behind")
	deliverSimRead(t, c)

	if len(c.readStates) != 1 {
		t.Fatal("confirmation was not collected")
	}

	if len(c.ReadStates()) != 0 {
		t.Fatal("unsatisfied barrier exposed")
	}

	if len(c.Nodes[leader].Ready().ReadStates) != 0 {
		t.Fatal("collected confirmation not acknowledged")
	}
}

func TestSimReadBarrierDrainRetainsPending(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	submitSimRead(t, c, leader, "retained")
	deliverSimRead(t, c)

	for i := 0; i < 10; i++ {
		if len(c.DrainReadStates()) != 0 {
			t.Fatal("pending barrier was drained")
		}

		if len(c.readStates) != 1 {
			t.Fatal("pending barrier was discarded")
		}
	}
}

func TestSimReadBarrierResumeDelivery(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	submitSimRead(t, c, leader, "resume")
	deliverSimRead(t, c)

	resumeSimApplication(t, c, leader)

	states := c.DrainReadStates()

	if len(states) != 1 {
		t.Fatal("resumed barrier was not delivered")
	}

	if requireSimAppliedIndex(t, c, leader) <
		states[0].State.Index {
		t.Fatal("read delivered before application")
	}

	if len(c.DrainReadStates()) != 0 {
		t.Fatal("resumed barrier delivered twice")
	}
}

func TestSimReadBarrierAlreadyApplied(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	pauseSimApplication(t, c, leader)

	submitSimRead(t, c, leader, "already-applied")
	deliverSimRead(t, c)

	if len(c.DrainReadStates()) != 1 {
		t.Fatal("satisfied barrier incorrectly blocked")
	}
}

func TestSimReadBarrierIndependentRequests(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	submitSimRead(t, c, leader, "earlier")
	deliverSimRead(t, c)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	submitSimRead(t, c, leader, "later")
	deliverSimRead(t, c)

	states := c.DrainReadStates()

	if len(states) != 1 ||
		string(states[0].State.Context) != "earlier" {
		t.Fatal("incorrect initial delivery")
	}

	if len(c.readStates) != 1 {
		t.Fatal("later confirmation was lost")
	}

	resumeSimApplication(t, c, leader)

	states = c.DrainReadStates()

	if len(states) != 1 ||
		string(states[0].State.Context) != "later" {
		t.Fatal("incorrect deferred delivery")
	}
}

func TestSimReadBarrierMultipleCommittedEntries(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	before := requireSimAppliedIndex(t, c, leader)

	pauseSimApplication(t, c, leader)

	for i := 0; i < 10; i++ {
		proposeSimBarrierEntry(t, c, leader)
	}

	app := c.applicationFor(leader)

	if len(app.pending) != 10 {
		t.Fatal("incorrect pending application count")
	}

	for i, entry := range app.pending {
		if entry.Index != before+uint64(i)+1 {
			t.Fatal("incorrect application ordering")
		}
	}

	resumeSimApplication(t, c, leader)

	if app.applied != before+10 ||
		len(app.pending) != 0 {
		t.Fatal("pending entries not applied correctly")
	}
}

func TestSimReadBarrierThreeNodes(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	submitSimRead(t, c, leader, "three-node")
	deliverSimRead(t, c)

	if len(c.readStates) != 1 ||
		len(c.ReadStates()) != 0 {
		t.Fatal("incorrect deferred quorum confirmation")
	}

	resumeSimApplication(t, c, leader)

	if len(c.DrainReadStates()) != 1 {
		t.Fatal("three-node read did not become ready")
	}

	faultTestSafety(t, c)
}

func TestSimReadBarrierFollowerPause(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	for _, id := range c.IDs {
		if id != leader {
			pauseSimApplication(t, c, id)
		}
	}

	proposeSimBarrierEntry(t, c, leader)

	submitSimRead(t, c, leader, "leader-ready")
	deliverSimRead(t, c)

	if len(c.DrainReadStates()) != 1 {
		t.Fatal("follower application blocked leader read")
	}
}

func TestSimReadBarrierLeadershipLoss(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	submitSimRead(t, c, leader, "obsolete")
	deliverSimRead(t, c)

	if len(c.readStates) != 1 {
		t.Fatal("expected deferred confirmation")
	}

	node := c.Nodes[leader]

	var peer raft.NodeID

	for _, id := range c.IDs {
		if id != leader {
			peer = id
			break
		}
	}

	if err := node.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: peer,
		To:   leader,
		Term: node.Status().Term + 1,
	}); err != nil {
		t.Fatal(err)
	}

	resumeSimApplication(t, c, leader)

	if len(c.DrainReadStates()) != 0 ||
		len(c.readStates) != 0 {
		t.Fatal("obsolete deferred confirmation survived")
	}
}

func TestSimReadBarrierResumeIdempotent(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	resumeSimApplication(t, c, leader)

	before := requireSimAppliedIndex(t, c, leader)

	for i := 0; i < 10; i++ {
		resumeSimApplication(t, c, leader)
	}

	if requireSimAppliedIndex(t, c, leader) != before {
		t.Fatal("resume applied entries twice")
	}
}

func TestSimReadBarrierNoncontiguousEntries(t *testing.T) {
	c := NewCluster([]raft.NodeID{1})

	err := c.acceptCommittedEntries(
		1,
		[]raft.Entry{
			{Index: 1, Term: 1},
			{Index: 3, Term: 1},
		},
	)

	if err == nil {
		t.Fatal("noncontiguous application accepted")
	}

	app := c.applicationFor(1)

	if app.applied != 0 ||
		len(app.pending) != 0 {
		t.Fatal("invalid batch partially applied")
	}
}

func TestSimReadBarrierEntryIsolation(t *testing.T) {
	c := NewCluster([]raft.NodeID{1})

	pauseSimApplication(t, c, 1)

	entries := []raft.Entry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("original"),
		},
	}

	if err := c.acceptCommittedEntries(
		1, entries,
	); err != nil {
		t.Fatal(err)
	}

	entries[0].Data[0] = 'X'
	entries[0].Index = 99

	pending := c.applicationFor(1).pending

	if pending[0].Index != 1 ||
		string(pending[0].Data) != "original" {
		t.Fatal("pending application was mutated")
	}
}
