package raftdriver

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

type recoveryTestNode struct {
	node    *raft.Raft
	storage *wal.WAL
	driver  *Driver
	store   *kvstore.Store
}

type recoveryTestCluster struct {
	t        *testing.T
	dir      string
	ids      []raft.NodeID
	nodes    map[raft.NodeID]*recoveryTestNode
	online   map[raft.NodeID]bool
	blocked  map[raft.NodeID]bool
	messages []raft.Message
}

func newRecoveryTestCluster(t *testing.T) *recoveryTestCluster {
	t.Helper()

	c := &recoveryTestCluster{
		t:       t,
		dir:     t.TempDir(),
		ids:     []raft.NodeID{1, 2, 3},
		nodes:   make(map[raft.NodeID]*recoveryTestNode),
		online:  make(map[raft.NodeID]bool),
		blocked: make(map[raft.NodeID]bool),
	}

	for _, id := range c.ids {
		c.start(id)
	}

	t.Cleanup(func() {
		for _, n := range c.nodes {
			_ = n.storage.Close()
		}
	})

	return c
}

func (c *recoveryTestCluster) start(id raft.NodeID) {
	c.t.Helper()

	var peers []raft.NodeID

	for _, other := range c.ids {
		if other != id {
			peers = append(peers, other)
		}
	}

	store := kvstore.New()

	node, storage, driver, err := RecoverDriver(
		filepath.Join(
			c.dir,
			fmt.Sprintf("node-%d.wal", id),
		),
		id,
		peers,
		5+int(id-1)*3,
		store.Apply,
		func(outgoing []raft.Message) error {
			c.messages = append(c.messages, outgoing...)
			return nil
		},
	)
	if err != nil {
		c.t.Fatal(err)
	}

	c.nodes[id] = &recoveryTestNode{
		node:    node,
		storage: storage,
		driver:  driver,
		store:   store,
	}

	c.online[id] = true

	// Replay committed entries before allowing
	// the recovered node to receive messages.
	if err := driver.Process(); err != nil {
		c.t.Fatal(err)
	}
}

func (c *recoveryTestCluster) crash(id raft.NodeID) {
	c.t.Helper()

	if !c.online[id] {
		c.t.Fatalf("node %d is already offline", id)
	}

	c.online[id] = false

	if err := c.nodes[id].storage.Close(); err != nil {
		c.t.Fatal(err)
	}

	// Drop messages associated with the crashed node.
	var remaining []raft.Message

	for _, msg := range c.messages {
		if msg.From != id && msg.To != id {
			remaining = append(remaining, msg)
		}
	}

	c.messages = remaining
}

func (c *recoveryTestCluster) restart(id raft.NodeID) {
	c.t.Helper()

	if c.online[id] {
		c.t.Fatalf("node %d is already online", id)
	}

	c.start(id)

	if c.nodes[id].node.Status().Role != raft.Follower {
		c.t.Fatalf(
			"node %d did not recover as follower",
			id,
		)
	}
}

// Process every online node before delivering messages.
// Repeat until the network becomes idle.
func (c *recoveryTestCluster) deliver() {
	c.t.Helper()

	for round := 0; round < 1000; round++ {
		for _, id := range c.ids {
			if !c.online[id] {
				continue
			}

			if err := c.nodes[id].driver.Process(); err != nil {
				c.t.Fatalf(
					"process node %d: %v",
					id,
					err,
				)
			}
		}

		if len(c.messages) == 0 {
			return
		}

		batch := c.messages
		c.messages = nil

		for _, msg := range batch {
			if !c.online[msg.From] || !c.online[msg.To] {
				continue
			}

			// Simulate bidirectional network isolation.
			if c.blocked[msg.From] || c.blocked[msg.To] {
				continue
			}

			if err := c.nodes[msg.To].node.Step(msg); err != nil {
				c.t.Fatalf(
					"message %d -> %d: %v",
					msg.From,
					msg.To,
					err,
				)
			}
		}
	}

	c.t.Fatal("message delivery did not settle")
}

func (c *recoveryTestCluster) advance(ticks int) {
	c.t.Helper()

	for i := 0; i < ticks; i++ {
		for _, id := range c.ids {
			if c.online[id] {
				c.nodes[id].node.Tick()
			}
		}

		c.deliver()
	}
}

func (c *recoveryTestCluster) leader() raft.NodeID {
	c.t.Helper()

	var leader raft.NodeID

	for _, id := range c.ids {
		if !c.online[id] || c.blocked[id] {
			continue
		}

		if c.nodes[id].node.Status().Role == raft.Leader {
			if leader != 0 {
				c.t.Fatal("multiple connected leaders")
			}

			leader = id
		}
	}

	return leader
}

func (c *recoveryTestCluster) propose(
	leader raft.NodeID,
	key, value string,
) {
	c.t.Helper()

	if !c.online[leader] {
		c.t.Fatalf("leader %d is offline", leader)
	}

	data, err := kvstore.EncodeCommand(kvstore.Command{
		Op:    kvstore.OpSet,
		Key:   key,
		Value: value,
	})
	if err != nil {
		c.t.Fatal(err)
	}

	if err := c.nodes[leader].node.Propose(data); err != nil {
		c.t.Fatal(err)
	}

	c.deliver()

	// Propagate the updated commit index.
	c.advance(2)
}

func (c *recoveryTestCluster) check(
	id raft.NodeID,
	key, expected string,
) {
	c.t.Helper()

	value, exists := c.nodes[id].store.Get(key)

	if !exists || value != expected {
		c.t.Fatalf(
			"node %d: key %q expected %q, got %q (exists=%v)",
			id,
			key,
			expected,
			value,
			exists,
		)
	}
}

func (c *recoveryTestCluster) checkAll(
	expected map[string]string,
) {
	c.t.Helper()

	for _, id := range c.ids {
		if !c.online[id] {
			continue
		}

		for key, value := range expected {
			c.check(id, key, value)
		}
	}
}

func (c *recoveryTestCluster) checkIndexes() {
	c.t.Helper()

	var expected uint64
	first := true

	for _, id := range c.ids {
		if !c.online[id] {
			continue
		}

		commit := c.nodes[id].node.Status().Commit
		applied := c.nodes[id].store.AppliedIndex()

		if applied != commit {
			c.t.Fatalf(
				"node %d: applied=%d, commit=%d",
				id,
				applied,
				commit,
			)
		}

		if first {
			expected = commit
			first = false
		} else if commit != expected {
			c.t.Fatalf(
				"node %d: commit=%d, expected=%d",
				id,
				commit,
				expected,
			)
		}
	}
}

// TEST 1: Follower crash and recovery.
func TestThreeNodeFollowerCrashAndRecovery(t *testing.T) {
	c := newRecoveryTestCluster(t)

	c.advance(6)

	leader := c.leader()
	if leader == 0 {
		t.Fatal("no initial leader")
	}

	t.Logf("initial leader: node %d", leader)

	c.propose(leader, "alpha", "10")
	c.checkAll(map[string]string{
		"alpha": "10",
	})

	var follower raft.NodeID

	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	c.crash(follower)
	t.Logf("crashed follower: node %d", follower)

	c.propose(leader, "beta", "20")

	c.restart(follower)
	c.check(follower, "alpha", "10")

	c.propose(leader, "gamma", "30")
	c.advance(4)

	c.checkAll(map[string]string{
		"alpha": "10",
		"beta":  "20",
		"gamma": "30",
	})

	c.checkIndexes()

	t.Log("three-node follower recovery verified")
}

// TEST 2: Leader crash, re-election, and recovery.
func TestThreeNodeLeaderCrashAndRecovery(t *testing.T) {
	c := newRecoveryTestCluster(t)

	c.advance(6)

	oldLeader := c.leader()
	if oldLeader == 0 {
		t.Fatal("no initial leader")
	}

	t.Logf("initial leader: node %d", oldLeader)

	c.propose(oldLeader, "before", "100")

	c.checkAll(map[string]string{
		"before": "100",
	})

	c.crash(oldLeader)
	t.Logf("crashed leader: node %d", oldLeader)

	c.advance(20)

	newLeader := c.leader()
	if newLeader == 0 {
		t.Fatal("no replacement leader elected")
	}

	if newLeader == oldLeader {
		t.Fatal("crashed leader was re-elected")
	}

	t.Logf("new leader: node %d", newLeader)

	c.propose(newLeader, "during", "200")

	for _, id := range c.ids {
		if c.online[id] {
			c.check(id, "before", "100")
			c.check(id, "during", "200")
		}
	}

	c.restart(oldLeader)

	t.Logf("restarted old leader: node %d", oldLeader)

	c.check(oldLeader, "before", "100")

	c.propose(newLeader, "after", "300")
	c.advance(6)

	c.checkAll(map[string]string{
		"before": "100",
		"during": "200",
		"after":  "300",
	})

	c.checkIndexes()

	if c.nodes[oldLeader].node.Status().Role != raft.Follower {
		t.Fatal("old leader should remain a follower")
	}

	if c.leader() != newLeader {
		t.Fatal("unexpected leader change after recovery")
	}

	t.Log("leader crash, re-election, and recovery verified")
}

// TEST 3: Network partition and reconciliation.
func TestThreeNodeNetworkPartitionAndReconciliation(t *testing.T) {
	c := newRecoveryTestCluster(t)

	// Phase 1: Establish the original leader.
	c.advance(6)

	oldLeader := c.leader()
	if oldLeader == 0 {
		t.Fatal("no initial leader")
	}

	t.Logf("initial leader: node %d", oldLeader)

	c.propose(oldLeader, "stable", "100")

	c.checkAll(map[string]string{
		"stable": "100",
	})

	originalCommit := c.nodes[oldLeader].node.Status().Commit

	// Phase 2: Isolate the leader.
	c.blocked[oldLeader] = true

	t.Logf("isolated leader: node %d", oldLeader)

	// The isolated leader appends a command locally.
	data, err := kvstore.EncodeCommand(kvstore.Command{
		Op:    kvstore.OpSet,
		Key:   "uncommitted",
		Value: "999",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.nodes[oldLeader].node.Propose(data); err != nil {
		t.Fatal(err)
	}

	c.deliver()

	// Without a majority, the isolated leader
	// must not advance its commit index.
	if got := c.nodes[oldLeader].node.Status().Commit; got != originalCommit {
		t.Fatalf(
			"isolated leader committed without majority: %d -> %d",
			originalCommit,
			got,
		)
	}

	if _, exists := c.nodes[oldLeader].store.Get("uncommitted"); exists {
		t.Fatal("uncommitted entry was applied")
	}

	t.Log("isolated leader could not commit")

	// Phase 3: The majority elects another leader.
	c.advance(20)

	newLeader := c.leader()
	if newLeader == 0 {
		t.Fatal("majority did not elect a replacement leader")
	}

	if newLeader == oldLeader {
		t.Fatal("isolated leader was selected as replacement")
	}

	t.Logf("replacement leader: node %d", newLeader)

	if c.nodes[newLeader].node.Status().Term <=
		c.nodes[oldLeader].node.Status().Term {
		t.Fatal("replacement leader must have a higher term")
	}

	// Phase 4: Majority commits new data.
	c.propose(newLeader, "majority", "200")

	for _, id := range c.ids {
		if id == oldLeader {
			continue
		}

		c.check(id, "stable", "100")
		c.check(id, "majority", "200")
	}

	if _, exists := c.nodes[oldLeader].store.Get("majority"); exists {
		t.Fatal("isolated node received majority data")
	}

	// Phase 5: Reconnect the isolated node.
	delete(c.blocked, oldLeader)

	t.Log("network partition healed")

	// A new proposal triggers AppendEntries
	// and reconciliation of conflicting logs.
	c.propose(newLeader, "reconciled", "300")
	c.advance(10)

	// Phase 6: Verify state machine consistency.
	c.checkAll(map[string]string{
		"stable":     "100",
		"majority":   "200",
		"reconciled": "300",
	})

	// The isolated leader's uncommitted entry
	// must never be applied.
	for _, id := range c.ids {
		if _, exists := c.nodes[id].store.Get("uncommitted"); exists {
			t.Fatalf(
				"node %d applied a discarded uncommitted entry",
				id,
			)
		}
	}

	c.checkIndexes()

	if c.nodes[oldLeader].node.Status().Role != raft.Follower {
		t.Fatal("old leader did not step down")
	}

	if c.leader() != newLeader {
		t.Fatal("unexpected leader after reconciliation")
	}

	t.Log("network partition and reconciliation verified")
}
