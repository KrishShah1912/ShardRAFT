package simnet

import (
	"bytes"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func faultTestLeader(t *testing.T, c *Cluster) raft.NodeID {
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

func faultTestFollowers(c *Cluster, leader raft.NodeID) []raft.NodeID {
	var followers []raft.NodeID

	for _, id := range c.IDs {
		if id != leader {
			followers = append(followers, id)
		}
	}

	return followers
}

func faultTestAdvance(t *testing.T, c *Cluster, ticks int) {
	t.Helper()

	for i := 0; i < ticks; i++ {
		c.Tick()
		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}
	}
}

func faultTestCommitted(
	t *testing.T,
	c *Cluster,
	ids []raft.NodeID,
	index uint64,
	expected []byte,
) bool {
	t.Helper()

	for _, id := range ids {
		entry, ok := c.Nodes[id].CommittedEntry(index)
		if !ok || !bytes.Equal(entry.Data, expected) {
			return false
		}
	}

	return true
}

func faultTestSafety(t *testing.T, c *Cluster) {
	t.Helper()

	for _, id := range c.IDs {
		if err := c.Nodes[id].CheckInvariants(); err != nil {
			t.Fatalf("node %d invariant violation: %v", id, err)
		}
	}

	// Compare overlapping committed entries on every pair.
	for i := 0; i < len(c.IDs); i++ {
		for j := i + 1; j < len(c.IDs); j++ {
			a := c.Nodes[c.IDs[i]]
			b := c.Nodes[c.IDs[j]]

			limit := a.Status().Commit
			if b.Status().Commit < limit {
				limit = b.Status().Commit
			}

			for index := uint64(1); index <= limit; index++ {
				ea, oka := a.CommittedEntry(index)
				eb, okb := b.CommittedEntry(index)

				if !oka || !okb {
					t.Fatalf("missing committed entry at index %d", index)
				}

				if ea.Term != eb.Term ||
					ea.Index != eb.Index ||
					ea.Type != eb.Type ||
					!bytes.Equal(ea.Data, eb.Data) {
					t.Fatalf(
						"committed-log mismatch at index %d between nodes %d and %d",
						index, c.IDs[i], c.IDs[j],
					)
				}
			}
		}
	}
}

// TestReplicationWithDroppedAcknowledgments verifies that a
// leader can commit using its majority despite missing responses
// from one follower.
func TestReplicationWithDroppedAcknowledgments(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})
	leader := faultTestLeader(t, c)
	followers := faultTestFollowers(c, leader)
	unreliable := followers[0]

	dropped := 0
	c.DropMessage = func(msg raft.Message) bool {
		if msg.Type == raft.MsgAppendResp &&
			msg.From == unreliable &&
			msg.To == leader {
			dropped++
			return true
		}
		return false
	}

	command := []byte("SET ack-loss=100")

	if err := c.Nodes[leader].Propose(command); err != nil {
		t.Fatal(err)
	}

	if err := c.Deliver(); err != nil {
		t.Fatal(err)
	}

	// The other follower provides a majority.
	if !faultTestCommitted(
		t, c, []raft.NodeID{leader}, 2, command,
	) {
		t.Fatal("leader failed to commit with a majority")
	}

	faultTestAdvance(t, c, 6)

	if dropped == 0 {
		t.Fatal("expected at least one dropped acknowledgment")
	}

	// Restore all acknowledgments.
	c.DropMessage = nil

	recovered := false
	for tick := 0; tick < 50; tick++ {
		faultTestAdvance(t, c, 1)

		if faultTestCommitted(t, c, c.IDs, 2, command) {
			recovered = true
			break
		}
	}

	if !recovered {
		t.Fatal("not all nodes committed after acknowledgment recovery")
	}

	faultTestSafety(t, c)
	t.Logf("recovered after dropping %d acknowledgments", dropped)
}

// TestReplicationWithReorderedAcknowledgments verifies that
// late acknowledgments do not prevent eventual replication.
func TestReplicationWithReorderedAcknowledgments(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})
	leader := faultTestLeader(t, c)
	followers := faultTestFollowers(c, leader)
	delayed := followers[0]

	c.HoldMessage = func(msg raft.Message) bool {
		return msg.Type == raft.MsgAppendResp &&
			msg.From == delayed &&
			msg.To == leader
	}

	first := []byte("SET reorder-a=10")
	if err := c.Nodes[leader].Propose(first); err != nil {
		t.Fatal(err)
	}
	if err := c.Deliver(); err != nil {
		t.Fatal(err)
	}

	second := []byte("SET reorder-b=20")
	if err := c.Nodes[leader].Propose(second); err != nil {
		t.Fatal(err)
	}
	if err := c.Deliver(); err != nil {
		t.Fatal(err)
	}

	faultTestAdvance(t, c, 8)

	if len(c.HeldMessages) < 2 {
		t.Fatalf(
			"expected at least two held acknowledgments, got %d",
			len(c.HeldMessages),
		)
	}

	held := len(c.HeldMessages)

	c.HoldMessage = nil

	if err := c.ReleaseHeldReverse(); err != nil {
		t.Fatal(err)
	}

	if err := c.Deliver(); err != nil {
		t.Fatal(err)
	}

	recovered := false

	for tick := 0; tick < 50; tick++ {
		faultTestAdvance(t, c, 1)

		if faultTestCommitted(t, c, c.IDs, 2, first) &&
			faultTestCommitted(t, c, c.IDs, 3, second) {
			recovered = true
			break
		}
	}

	if !recovered {
		t.Fatal("commands did not converge after reordered acknowledgments")
	}

	faultTestSafety(t, c)
	t.Logf("recovered after reversing %d acknowledgments", held)
}
