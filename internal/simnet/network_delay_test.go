package simnet

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func electNetworkTestLeader(t *testing.T, c *Cluster) raft.NodeID {
	t.Helper()

	for tick := 0; tick < 20; tick++ {
		c.Tick()
		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}

		for _, id := range c.IDs {
			if c.Nodes[id].Status().Role == raft.Leader {
				return id
			}
		}
	}

	t.Fatal("no leader elected")
	return 0
}

// TestDelayedAppendResponses verifies that acknowledgments
// can be held while the rest of the network continues operating.
func TestDelayedAppendResponses(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})
	leader := electNetworkTestLeader(t, c)

	var follower raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			follower = id
			break
		}
	}

	c.HoldMessage = func(msg raft.Message) bool {
		return msg.Type == raft.MsgAppendResp &&
			msg.From == follower &&
			msg.To == leader
	}

	for i := 0; i < 10; i++ {
		c.Tick()
		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}
	}

	if len(c.HeldMessages) == 0 {
		t.Fatal("expected delayed AppendEntries responses")
	}

	held := len(c.HeldMessages)
	t.Logf("held %d AppendEntries responses", held)

	// Stop holding new responses before releasing old ones.
	c.HoldMessage = nil

	if err := c.ReleaseHeld(); err != nil {
		t.Fatal(err)
	}

	if err := c.Deliver(); err != nil {
		t.Fatal(err)
	}

	if len(c.HeldMessages) != 0 {
		t.Fatal("held message queue was not cleared")
	}

	for _, id := range c.IDs {
		if err := c.Nodes[id].CheckInvariants(); err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
	}
}

// TestOutOfOrderAppendResponses verifies that delayed
// acknowledgments can be delivered in reverse order.
func TestOutOfOrderAppendResponses(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})
	leader := electNetworkTestLeader(t, c)

	var follower raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			follower = id
			break
		}
	}

	c.HoldMessage = func(msg raft.Message) bool {
		return msg.Type == raft.MsgAppendResp &&
			msg.From == follower &&
			msg.To == leader
	}

	for i := 0; i < 12; i++ {
		c.Tick()
		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}
	}

	if len(c.HeldMessages) < 2 {
		t.Fatalf(
			"expected multiple delayed responses, got %d",
			len(c.HeldMessages),
		)
	}

	t.Logf(
		"reordering %d delayed AppendEntries responses",
		len(c.HeldMessages),
	)

	c.HoldMessage = nil

	if err := c.ReleaseHeldReverse(); err != nil {
		t.Fatal(err)
	}

	if err := c.Deliver(); err != nil {
		t.Fatal(err)
	}

	// Continue normal network operation after reordering.
	for i := 0; i < 20; i++ {
		c.Tick()
		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}
	}

	for _, id := range c.IDs {
		if err := c.Nodes[id].CheckInvariants(); err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
	}

	t.Log("out-of-order response handling passed")
}
