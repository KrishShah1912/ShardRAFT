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

func TestThreeNodeReplication(t *testing.T) {
	cluster := NewCluster([]raft.NodeID{1, 2, 3})

	// Elect a leader.
	for i := 0; i < 20; i++ {
		cluster.Tick()

		if err := cluster.Deliver(); err != nil {
			t.Fatal(err)
		}
	}

	var leader *raft.Raft

	for _, id := range cluster.IDs {
		node := cluster.Nodes[id]

		if node.Status().Role == raft.Leader {
			leader = node
			break
		}
	}

	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Propose a command through the elected leader.
	if err := leader.Propose([]byte("SET x=10")); err != nil {
		t.Fatal(err)
	}

	// Deliver real AppendEntries messages and responses.
	if err := cluster.Deliver(); err != nil {
		t.Fatal(err)
	}

	// A majority should allow the leader to commit.
	if leader.Status().Commit != 2 {
		t.Fatalf("expected leader commit index 2, got %d", leader.Status().Commit)
	}
}
