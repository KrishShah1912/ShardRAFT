package simnet

import (
	"bytes"
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func newReadIndexTestCluster(
	t *testing.T,
	ids []raft.NodeID,
) (*Cluster, raft.NodeID) {
	t.Helper()

	c := NewCluster(ids)

	for tick := 0; tick < 50; tick++ {
		c.Tick()

		if err := c.Deliver(); err != nil {
			t.Fatal(err)
		}

		for _, id := range c.IDs {
			status := c.Nodes[id].Status()

			if status.Role != raft.Leader ||
				status.Commit == 0 {
				continue
			}

			term, exists := c.Nodes[id].TermAt(
				status.Commit,
			)

			if exists && term == status.Term {
				return c, id
			}
		}
	}

	t.Fatal("no read-ready leader elected")

	return nil, 0
}

func TestSimReadAdmissionUnknownNode(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})

	if err := c.ReadIndex(99, []byte("read")); err == nil {
		t.Fatal("unknown node accepted read")
	}
}

func TestSimReadAdmissionNilNode(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})

	c.Nodes[99] = nil

	if err := c.ReadIndex(99, []byte("read")); err == nil {
		t.Fatal("nil node accepted read")
	}
}

func TestSimReadAdmissionFollower(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	for _, id := range c.IDs {
		if id == leader {
			continue
		}

		err := c.ReadIndex(id, []byte("follower-read"))

		if !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf(
				"expected ErrNotLeader, got %v", err,
			)
		}
	}
}

func TestSimReadAdmissionBeforeElection(t *testing.T) {
	c := NewCluster([]raft.NodeID{1, 2, 3})

	for _, id := range c.IDs {
		err := c.ReadIndex(id, []byte("early-read"))

		if !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf(
				"expected ErrNotLeader, got %v", err,
			)
		}
	}
}

func TestSimReadAdmissionInvalidContext(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	contexts := [][]byte{
		nil,
		{},
		make([]byte, 4097),
	}

	for _, ctx := range contexts {
		err := c.ReadIndex(leader, ctx)

		if !errors.Is(err, raft.ErrInvalidReadContext) {
			t.Fatalf(
				"expected ErrInvalidReadContext, got %v",
				err,
			)
		}
	}
}

func TestSimReadAdmissionThreeNodes(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	if err := c.ReadIndex(
		leader, []byte("three-node-read"),
	); err != nil {
		t.Fatal(err)
	}

	ready := c.Nodes[leader].Ready()

	if len(ready.ReadStates) != 0 {
		t.Fatal("read completed before network delivery")
	}

	probes := 0

	for _, msg := range ready.Messages {
		if msg.Type == raft.MsgHeartbeat &&
			msg.RequestID != 0 {
			probes++
		}
	}

	if probes != 2 {
		t.Fatalf("expected 2 probes, got %d", probes)
	}
}

func TestSimReadAdmissionFiveNodes(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3, 4, 5},
	)

	if err := c.ReadIndex(
		leader, []byte("five-node-read"),
	); err != nil {
		t.Fatal(err)
	}

	ready := c.Nodes[leader].Ready()

	if len(ready.ReadStates) != 0 {
		t.Fatal("read completed without network delivery")
	}

	probes := 0

	for _, msg := range ready.Messages {
		if msg.Type == raft.MsgHeartbeat &&
			msg.RequestID != 0 {
			probes++
		}
	}

	if probes != 4 {
		t.Fatalf("expected 4 probes, got %d", probes)
	}
}

func TestSimReadAdmissionSingleNode(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	ctx := []byte("single-node-read")

	if err := c.ReadIndex(leader, ctx); err != nil {
		t.Fatal(err)
	}

	ready := c.Nodes[leader].Ready()

	if len(ready.ReadStates) != 1 {
		t.Fatal("expected one self-quorum confirmation")
	}

	state := ready.ReadStates[0]
	status := c.Nodes[leader].Status()

	if !bytes.Equal(state.Context, ctx) ||
		state.RequestID == 0 ||
		state.Term != status.Term ||
		state.Index != status.Commit {
		t.Fatalf("invalid read confirmation: %+v", state)
	}

	if len(ready.Messages) != 0 {
		t.Fatal("single-node read generated network traffic")
	}
}

func TestSimReadAdmissionContextIsolation(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1},
	)

	ctx := []byte("original-context")

	if err := c.ReadIndex(leader, ctx); err != nil {
		t.Fatal(err)
	}

	ctx[0] = 'X'

	states := c.Nodes[leader].Ready().ReadStates

	if len(states) != 1 {
		t.Fatal("expected one read confirmation")
	}

	if string(states[0].Context) != "original-context" {
		t.Fatal("caller mutation changed read context")
	}
}

func TestSimReadAdmissionConcurrentRequests(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	for _, ctx := range []string{
		"first",
		"second",
		"third",
	} {
		if err := c.ReadIndex(
			leader, []byte(ctx),
		); err != nil {
			t.Fatal(err)
		}
	}

	counts := make(map[uint64]int)

	for _, msg := range c.Nodes[leader].Ready().Messages {
		if msg.Type == raft.MsgHeartbeat &&
			msg.RequestID != 0 {
			counts[msg.RequestID]++
		}
	}

	if len(counts) != 3 {
		t.Fatalf(
			"expected 3 distinct requests, got %d",
			len(counts),
		)
	}

	for id, count := range counts {
		if count != 2 {
			t.Fatalf(
				"request %d generated %d probes",
				id, count,
			)
		}
	}
}

func TestSimReadAdmissionNoAutomaticDelivery(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	delivered := 0

	c.DropMessage = func(msg raft.Message) bool {
		delivered++
		return false
	}

	if err := c.ReadIndex(
		leader, []byte("explicit-delivery"),
	); err != nil {
		t.Fatal(err)
	}

	if delivered != 0 {
		t.Fatal("admission unexpectedly delivered messages")
	}

	if len(c.Nodes[leader].Ready().ReadStates) != 0 {
		t.Fatal("read unexpectedly completed")
	}

	if err := c.Deliver(); err != nil {
		t.Fatal(err)
	}

	if delivered == 0 {
		t.Fatal("explicit delivery did not process messages")
	}

	// Confirmations are now owned by the simulator.
	states := c.ReadStates()

	if len(states) != 1 {
		t.Fatal("expected one quorum-confirmed read")
	}

	if states[0].NodeID != leader {
		t.Fatal("incorrect confirmation source")
	}

	if len(c.Nodes[leader].Ready().ReadStates) != 0 {
		t.Fatal("collected confirmation was not acknowledged")
	}
}

func TestSimReadAdmissionNoDurableChanges(t *testing.T) {
	c, leader := newReadIndexTestCluster(
		t, []raft.NodeID{1, 2, 3},
	)

	node := c.Nodes[leader]
	before := node.Status()

	for i := 0; i < 100; i++ {
		if err := c.ReadIndex(
			leader, []byte("read-only"),
		); err != nil {
			t.Fatal(err)
		}
	}

	after := node.Status()
	ready := node.Ready()

	if before != after {
		t.Fatal("read admission changed Raft status")
	}

	if ready.HardState != nil ||
		len(ready.Entries) != 0 ||
		len(ready.CommittedEntries) != 0 ||
		ready.TruncateFrom != 0 {
		t.Fatal("read admission generated durable work")
	}

	if err := node.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}
