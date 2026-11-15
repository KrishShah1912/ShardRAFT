package raftdriver

import (
	"fmt"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// TestReplicationRecoveryAfterExtendedPartition verifies that
// a follower catches up after an extended network partition,
// without requiring another client proposal after healing.
func TestReplicationRecoveryAfterExtendedPartition(t *testing.T) {
	c := newRecoveryTestCluster(t)
	c.advance(6)

	leader := c.leader()
	if leader == 0 {
		t.Fatal("no initial leader")
	}

	// Select one follower to isolate.
	var follower raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	// Commit an initial command on all nodes.
	c.propose(leader, "baseline", "ready")
	c.checkAll(map[string]string{"baseline": "ready"})

	// Isolate the follower.
	c.blocked[follower] = true

	// Generate multiple committed entries while isolated.
	expected := make(map[string]string)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("partition-key-%d", i)
		value := fmt.Sprintf("value-%d", i)

		c.propose(leader, key, value)
		expected[key] = value
	}

	// The isolated follower must not have received
	// the commands proposed during the partition.
	for key := range expected {
		if _, exists := c.nodes[follower].store.Get(key); exists {
			t.Fatalf("isolated follower unexpectedly has %q", key)
		}
	}

	// Keep the partition active through additional
	// heartbeat and replication retry cycles.
	c.advance(30)

	// Restore network connectivity.
	delete(c.blocked, follower)

	// No new proposals after healing. The leader must
	// recover the follower using replication retries.
	recovered := false

	for tick := 0; tick < 100; tick++ {
		c.advance(1)

		allPresent := true
		for key, value := range expected {
			got, exists := c.nodes[follower].store.Get(key)
			if !exists || got != value {
				allPresent = false
				break
			}
		}

		if allPresent {
			recovered = true
			t.Logf("follower %d recovered after %d ticks", follower, tick+1)
			break
		}
	}

	if !recovered {
		t.Fatal("follower did not catch up after network recovery")
	}

	// Verify all state machines agree.
	c.checkAll(expected)
	c.checkAll(map[string]string{"baseline": "ready"})

	// Verify committed entries agree across nodes.
	checkCommittedLogConsistency(t, c)

	// Verify Raft's internal safety invariants.
	for _, id := range c.ids {
		if err := c.nodes[id].node.CheckInvariants(); err != nil {
			t.Fatalf("node %d invariant violation: %v", id, err)
		}
	}

	t.Log("extended partition recovery and committed-log consistency passed")
}
