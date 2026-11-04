package simnet

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestThreeNodeElection(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})

	for tick := 0; tick < 20; tick++ {
		c.Tick()

		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}
	}

	leaders := 0

	for _, id := range c.IDs {
		node := c.Nodes[id]

		if node.Status().Role == raft.Leader {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf("expected exactly one leader, got %d", leaders)
	}
}
