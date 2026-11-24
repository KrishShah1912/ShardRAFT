package simnet

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func isSimReadProbe(msg raft.Message) bool {
	return msg.Type == raft.MsgHeartbeat &&
		msg.RequestID != 0
}

func isSimReadAck(msg raft.Message) bool {
	return msg.Type == raft.MsgHeartbeatResp &&
		msg.RequestID != 0
}

func tickSimReadNetwork(
	t *testing.T,
	c *Cluster,
	count int,
) {
	t.Helper()

	for i := 0; i < count; i++ {
		c.Tick()
		deliverSimRead(t, c)
	}
}

func requireSimReadCount(
	t *testing.T,
	c *Cluster,
	expected int,
) {
	t.Helper()

	if actual := len(c.ReadStates()); actual != expected {
		t.Fatalf(
			"expected %d confirmations, got %d",
			expected, actual,
		)
	}
}

func requireSimReadContext(
	t *testing.T,
	c *Cluster,
	leader raft.NodeID,
	ctx string,
) SimReadState {
	t.Helper()

	states := c.DrainReadStates()

	if len(states) != 1 {
		t.Fatalf(
			"expected one confirmation, got %d",
			len(states),
		)
	}

	state := states[0]

	if state.NodeID != leader ||
		string(state.State.Context) != ctx {
		t.Fatalf(
			"incorrect confirmation: %+v",
			state,
		)
	}

	if requireSimAppliedIndex(t, c, leader) <
		state.State.Index {
		t.Fatal("read exposed before application")
	}

	return state
}

func TestSimReadFaultDroppedProbes(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.DropMessage = isSimReadProbe

	submitSimRead(t, c, leader, "dropped-probes")
	deliverSimRead(t, c)
	tickSimReadNetwork(t, c, 10)

	requireSimReadCount(t, c, 0)

	faultTestSafety(t, c)
}

func TestSimReadFaultDroppedAcknowledgments(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.DropMessage = isSimReadAck

	submitSimRead(t, c, leader, "dropped-acks")
	deliverSimRead(t, c)
	tickSimReadNetwork(t, c, 10)

	requireSimReadCount(t, c, 0)

	faultTestSafety(t, c)
}

func TestSimReadFaultOneFollowerUnavailable(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	var unavailable raft.NodeID

	for _, id := range c.IDs {
		if id != leader {
			unavailable = id
			break
		}
	}

	c.DropMessage = func(msg raft.Message) bool {
		return msg.From == unavailable ||
			msg.To == unavailable
	}

	submitSimRead(t, c, leader, "majority")
	deliverSimRead(t, c)

	requireSimReadContext(t, c, leader, "majority")

	faultTestSafety(t, c)
}

func TestSimReadFaultIsolatedLeader(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.DropMessage = func(msg raft.Message) bool {
		return msg.From == leader ||
			msg.To == leader
	}

	submitSimRead(t, c, leader, "isolated")
	deliverSimRead(t, c)
	tickSimReadNetwork(t, c, 20)

	requireSimReadCount(t, c, 0)

	faultTestSafety(t, c)
}

func TestSimReadFaultProbeRecovery(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.DropMessage = isSimReadProbe

	submitSimRead(t, c, leader, "probe-recovery")
	deliverSimRead(t, c)

	requireSimReadCount(t, c, 0)

	c.DropMessage = nil

	tickSimReadNetwork(t, c, 10)

	requireSimReadContext(
		t, c, leader, "probe-recovery",
	)

	faultTestSafety(t, c)
}

func TestSimReadFaultAcknowledgmentRecovery(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.DropMessage = isSimReadAck

	submitSimRead(t, c, leader, "ack-recovery")
	deliverSimRead(t, c)

	requireSimReadCount(t, c, 0)

	c.DropMessage = nil

	tickSimReadNetwork(t, c, 10)

	requireSimReadContext(
		t, c, leader, "ack-recovery",
	)
}

func TestSimReadFaultDelayedAcknowledgments(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.HoldMessage = isSimReadAck

	submitSimRead(t, c, leader, "delayed")
	deliverSimRead(t, c)

	requireSimReadCount(t, c, 0)

	if len(c.HeldMessages) != 2 {
		t.Fatalf(
			"expected 2 held acknowledgments, got %d",
			len(c.HeldMessages),
		)
	}

	c.HoldMessage = nil

	if err := c.ReleaseHeld(); err != nil {
		t.Fatal(err)
	}

	deliverSimRead(t, c)

	requireSimReadContext(t, c, leader, "delayed")
}

func TestSimReadFaultDuplicateAcknowledgments(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3, 4, 5},
	)

	c.HoldMessage = isSimReadAck

	submitSimRead(t, c, leader, "duplicates")
	deliverSimRead(t, c)

	if len(c.HeldMessages) != 4 {
		t.Fatal("expected four follower acknowledgments")
	}

	first := c.HeldMessages[0]
	second := c.HeldMessages[1]

	if first.From == second.From {
		t.Fatal("expected distinct followers")
	}

	for i := 0; i < 20; i++ {
		if err := c.deliverMessage(first); err != nil {
			t.Fatal(err)
		}
	}

	deliverSimRead(t, c)

	// Leader plus one distinct follower is not
	// a majority in a five-node cluster.
	requireSimReadCount(t, c, 0)

	if err := c.deliverMessage(second); err != nil {
		t.Fatal(err)
	}

	deliverSimRead(t, c)

	requireSimReadContext(
		t, c, leader, "duplicates",
	)

	c.HoldMessage = nil

	if err := c.ReleaseHeld(); err != nil {
		t.Fatal(err)
	}

	deliverSimRead(t, c)

	requireSimReadCount(t, c, 0)
}

func TestSimReadFaultReorderedAcknowledgments(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.HoldMessage = isSimReadAck

	for _, ctx := range []string{
		"first",
		"second",
		"third",
	} {
		submitSimRead(t, c, leader, ctx)
	}

	deliverSimRead(t, c)

	requireSimReadCount(t, c, 0)

	c.HoldMessage = nil

	if err := c.ReleaseHeldReverse(); err != nil {
		t.Fatal(err)
	}

	deliverSimRead(t, c)

	states := c.DrainReadStates()

	if len(states) != 3 {
		t.Fatal("reordering lost confirmations")
	}

	contexts := make(map[string]bool)
	ids := make(map[uint64]bool)

	for _, item := range states {
		if item.NodeID != leader {
			t.Fatal("incorrect confirmation source")
		}

		if ids[item.State.RequestID] {
			t.Fatal("duplicate confirmation identity")
		}

		ids[item.State.RequestID] = true
		contexts[string(item.State.Context)] = true
	}

	for _, ctx := range []string{
		"first",
		"second",
		"third",
	} {
		if !contexts[ctx] {
			t.Fatalf("missing context %q", ctx)
		}
	}
}

func findReplacementReadLeader(
	t *testing.T,
	c *Cluster,
	old raft.NodeID,
) raft.NodeID {
	t.Helper()

	oldTerm := c.Nodes[old].Status().Term

	for i := 0; i < 100; i++ {
		c.Tick()
		deliverSimRead(t, c)

		for _, id := range c.IDs {
			if id == old {
				continue
			}

			node := c.Nodes[id]
			status := node.Status()

			if status.Role != raft.Leader ||
				status.Term <= oldTerm ||
				status.Commit == 0 {
				continue
			}

			term, ok := node.TermAt(status.Commit)

			if ok && term == status.Term {
				return id
			}
		}
	}

	t.Fatal("replacement read-ready leader not elected")

	return 0
}

func TestSimReadFaultLeaderReplacement(t *testing.T) {
	c, old := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.DropMessage = func(msg raft.Message) bool {
		return msg.From == old ||
			msg.To == old
	}

	submitSimRead(t, c, old, "old-leader")
	deliverSimRead(t, c)

	replacement := findReplacementReadLeader(
		t, c, old,
	)

	requireSimReadCount(t, c, 0)

	submitSimRead(
		t, c, replacement, "new-leader",
	)

	deliverSimRead(t, c)

	requireSimReadContext(
		t, c, replacement, "new-leader",
	)

	c.DropMessage = nil

	tickSimReadNetwork(t, c, 20)

	if c.Nodes[old].Status().Role == raft.Leader {
		t.Fatal("obsolete leader did not step down")
	}

	requireSimReadCount(t, c, 0)

	faultTestSafety(t, c)
}

func TestSimReadFaultOldTermAcknowledgments(t *testing.T) {
	c, old := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.HoldMessage = isSimReadAck

	submitSimRead(t, c, old, "old-term")
	deliverSimRead(t, c)

	if len(c.HeldMessages) == 0 {
		t.Fatal("missing old-term acknowledgments")
	}

	c.HoldMessage = nil

	c.DropMessage = func(msg raft.Message) bool {
		return msg.From == old ||
			msg.To == old
	}

	replacement := findReplacementReadLeader(
		t, c, old,
	)

	c.DropMessage = nil

	tickSimReadNetwork(t, c, 10)

	if c.Nodes[old].Status().Role == raft.Leader {
		t.Fatal("old leader did not step down")
	}

	if err := c.ReleaseHeld(); err != nil {
		t.Fatal(err)
	}

	deliverSimRead(t, c)

	requireSimReadCount(t, c, 0)

	submitSimRead(
		t, c, replacement, "current-term",
	)

	deliverSimRead(t, c)

	requireSimReadContext(
		t, c, replacement, "current-term",
	)

	faultTestSafety(t, c)
}

func TestSimReadFaultConcurrentRecovery(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	c.DropMessage = isSimReadAck

	for i := 0; i < 100; i++ {
		submitSimRead(t, c, leader, "concurrent")
	}

	deliverSimRead(t, c)

	requireSimReadCount(t, c, 0)

	c.DropMessage = nil

	tickSimReadNetwork(t, c, 10)

	states := c.DrainReadStates()

	if len(states) != 100 {
		t.Fatalf(
			"expected 100 confirmations, got %d",
			len(states),
		)
	}

	ids := make(map[uint64]bool)

	for _, item := range states {
		if ids[item.State.RequestID] {
			t.Fatal("duplicate recovered confirmation")
		}

		ids[item.State.RequestID] = true

		if item.NodeID != leader ||
			string(item.State.Context) != "concurrent" {
			t.Fatal("incorrect recovered confirmation")
		}
	}

	tickSimReadNetwork(t, c, 10)

	requireSimReadCount(t, c, 0)

	faultTestSafety(t, c)
}

func TestSimReadFaultRecoveryWithApplicationLag(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	pauseSimApplication(t, c, leader)
	proposeSimBarrierEntry(t, c, leader)

	c.DropMessage = isSimReadAck

	submitSimRead(t, c, leader, "double-barrier")
	deliverSimRead(t, c)

	requireSimReadCount(t, c, 0)

	c.DropMessage = nil

	tickSimReadNetwork(t, c, 10)

	if len(c.readStates) != 1 {
		t.Fatal("quorum confirmation not collected")
	}

	requireSimReadCount(t, c, 0)

	if len(c.DrainReadStates()) != 0 {
		t.Fatal("unapplied confirmation drained")
	}

	resumeSimApplication(t, c, leader)

	requireSimReadContext(
		t, c, leader, "double-barrier",
	)

	faultTestSafety(t, c)
}

func TestSimReadFaultFiveNodeMinority(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3, 4, 5},
	)

	var reachable raft.NodeID

	for _, id := range c.IDs {
		if id != leader {
			reachable = id
			break
		}
	}

	c.DropMessage = func(msg raft.Message) bool {
		if isSimReadAck(msg) {
			return msg.From != reachable
		}

		return false
	}

	submitSimRead(t, c, leader, "minority")
	deliverSimRead(t, c)
	tickSimReadNetwork(t, c, 10)

	requireSimReadCount(t, c, 0)

	c.DropMessage = nil

	tickSimReadNetwork(t, c, 10)

	requireSimReadContext(t, c, leader, "minority")

	faultTestSafety(t, c)
}
