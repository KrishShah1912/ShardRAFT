package simnet

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// TestSelectiveAppendMessageDrop verifies that the simulator
// can selectively discard AppendEntries messages while
// preserving delivery to other nodes.
func TestSelectiveAppendMessageDrop(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})

	// Elect a leader.
	for i := 0; i < 10; i++ {
		c.Tick()

		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}
	}

	var leader raft.NodeID

	for _, id := range c.IDs {
		if c.Nodes[id].Status().Role == raft.Leader {
			leader = id
			break
		}
	}

	if leader == 0 {
		t.Fatal("no leader elected")
	}

	// Choose a follower for selective message loss.
	var isolated raft.NodeID

	for _, id := range c.IDs {
		if id != leader {
			isolated = id
			break
		}
	}

	dropped := 0

	// Drop only AppendEntries requests to the selected follower.
	c.DropMessage = func(msg raft.Message) bool {
		if msg.Type == raft.MsgAppend && msg.To == isolated {
			dropped++
			return true
		}

		return false
	}

	// Generate repeated replication attempts.
	for i := 0; i < 20; i++ {
		c.Tick()

		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}
	}

	if dropped == 0 {
		t.Fatal("expected AppendEntries messages to be dropped")
	}

	t.Logf(
		"dropped %d AppendEntries messages to follower %d",
		dropped,
		isolated,
	)

	// Restore normal network delivery.
	c.DropMessage = nil

	for i := 0; i < 20; i++ {
		c.Tick()

		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}
	}

	// Validate internal Raft invariants after recovery.
	for _, id := range c.IDs {
		if err := c.Nodes[id].CheckInvariants(); err != nil {
			t.Fatalf(
				"node %d invariant violation: %v",
				id,
				err,
			)
		}
	}

	t.Log("selective message loss test passed")
}
