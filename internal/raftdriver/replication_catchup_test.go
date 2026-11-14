package raftdriver

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestFollowerCatchesUpAfterPartition(t *testing.T) {
	c := newRecoveryTestCluster(t)

	c.advance(6)

	leader := c.leader()
	if leader == 0 {
		t.Fatal("no initial leader")
	}

	// Commit initial data on all nodes.
	c.propose(leader, "initial", "100")

	// Isolate one follower.
	var follower raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	c.blocked[follower] = true

	// The connected majority commits new entries.
	c.propose(leader, "second", "200")
	c.propose(leader, "third", "300")

	// Verify the isolated follower missed the data.
	if _, ok := c.nodes[follower].store.Get("third"); ok {
		t.Fatal("isolated follower unexpectedly received data")
	}

	// Heal the partition.
	delete(c.blocked, follower)

	// Give the cluster time to replicate.
	c.advance(30)

	// The follower must eventually catch up
	// without requiring another client proposal.
	c.check(follower, "initial", "100")
	c.check(follower, "second", "200")
	c.check(follower, "third", "300")

	t.Log("follower caught up after network recovery")
}
