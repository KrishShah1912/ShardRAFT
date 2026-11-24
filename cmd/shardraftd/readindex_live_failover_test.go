package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func replaceLiveReadIndexLeader(
	t *testing.T,
	c *liveReadIndexCluster,
	oldLeader *liveReadIndexNode,
	newID raft.NodeID,
) *liveReadIndexNode {
	t.Helper()

	before, err := oldLeader.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	if before.Role != raft.Leader {
		t.Fatal("expected original leader")
	}

	next := c.elect(t, newID)

	after, err := next.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	if after.Role != raft.Leader ||
		after.Term <= before.Term {
		t.Fatalf(
			"invalid leadership transition: before=%+v after=%+v",
			before,
			after,
		)
	}

	waitLiveReadIndexCondition(
		t,
		"previous leader steps down",
		func() bool {
			if err := next.shard.Tick(); err != nil {
				t.Fatal(err)
			}

			st, err := oldLeader.shard.Status()
			if err != nil {
				t.Fatal(err)
			}

			return st.Role == raft.Follower &&
				st.Term == after.Term &&
				st.LeaderID == next.id
		},
	)

	t.Logf(
		"Leadership transferred: node %d term %d -> node %d term %d",
		oldLeader.id,
		before.Term,
		next.id,
		after.Term,
	)

	return next
}

func requireLiveFormerLeaderRejection(
	t *testing.T,
	node *liveReadIndexNode,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	value, found, err :=
		node.shard.GetLinearizable(ctx, "alpha")

	if !errors.Is(err, raft.ErrNotLeader) ||
		found ||
		value != "" {
		t.Fatalf(
			"former leader %d returned value=%q found=%v err=%v",
			node.id,
			value,
			found,
			err,
		)
	}
}

func TestLiveReadIndexFailoverLeaderReplacement(
	t *testing.T,
) {
	c, oldLeader := newLiveReadIndexLeader(t)

	next := replaceLiveReadIndexLeader(
		t, c, oldLeader, 2,
	)

	requireLiveQuorumValue(
		t, next, "alpha", "100", true,
	)

	t.Log("PASS: Replacement leader served committed value.")
}

func TestLiveReadIndexFailoverPreviousLeaderRejection(
	t *testing.T,
) {
	c, oldLeader := newLiveReadIndexLeader(t)

	next := replaceLiveReadIndexLeader(
		t, c, oldLeader, 2,
	)

	for i := 0; i < 100; i++ {
		requireLiveFormerLeaderRejection(
			t, oldLeader,
		)
	}

	requireLiveQuorumValue(
		t, next, "alpha", "100", true,
	)

	t.Log("PASS: Former leader rejected all 100 reads.")
}

func TestLiveReadIndexFailoverLatestValue(
	t *testing.T,
) {
	c, oldLeader := newLiveReadIndexLeader(t)

	next := replaceLiveReadIndexLeader(
		t, c, oldLeader, 2,
	)

	putLiveQuorumValue(
		t, c, next, "alpha", "200",
	)

	requireLiveQuorumValue(
		t, next, "alpha", "200", true,
	)

	requireLiveFormerLeaderRejection(
		t, oldLeader,
	)

	t.Log("PASS: New leader returned latest committed value.")
}

func TestLiveReadIndexFailoverMultipleLeadershipChanges(
	t *testing.T,
) {
	c, leader := newLiveReadIndexLeader(t)

	for _, change := range []struct {
		id    raft.NodeID
		value string
	}{
		{2, "200"},
		{3, "300"},
		{1, "400"},
	} {
		previous := leader

		leader = replaceLiveReadIndexLeader(
			t, c, previous, change.id,
		)

		putLiveQuorumValue(
			t, c, leader, "alpha", change.value,
		)

		requireLiveQuorumValue(
			t, leader, "alpha", change.value, true,
		)

		requireLiveFormerLeaderRejection(
			t, previous,
		)
	}

	t.Log("PASS: Three successive leadership changes validated.")
}

func TestLiveReadIndexFailoverStaleLeader(
	t *testing.T,
) {
	c, oldLeader := newLiveReadIndexLeader(t)

	before, err := oldLeader.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	// Disable inbound Raft RPCs to the old leader.
	// Keep its shard and outbound transport running.
	// It cannot receive votes, append responses or
	// correlated heartbeat confirmations.
	oldLeader.server.Stop()

	t.Log("Old leader inbound gRPC server stopped.")

	next := c.elect(t, 2)

	after, err := next.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	if after.Term <= before.Term {
		t.Fatalf(
			"expected higher term: old=%d new=%d",
			before.Term,
			after.Term,
		)
	}

	err = next.shard.Propose(kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   "alpha",
		Value: "200",
	})
	if err != nil {
		t.Fatal(err)
	}

	waitLiveReadIndexCondition(
		t,
		"new majority commits latest value",
		func() bool {
			if err := next.shard.Tick(); err != nil {
				t.Fatal(err)
			}

			for _, id := range []raft.NodeID{2, 3} {
				value, found, err :=
					c.nodes[id].shard.Get("alpha")

				if err != nil {
					t.Fatal(err)
				}

				if !found || value != "200" {
					return false
				}
			}

			return true
		},
	)

	requireLiveQuorumValue(
		t, next, "alpha", "200", true,
	)

	stale, found, err :=
		oldLeader.shard.Get("alpha")

	if err != nil || !found || stale != "100" {
		t.Fatalf(
			"expected stale local value: value=%q found=%v err=%v",
			stale,
			found,
			err,
		)
	}

	oldStatus, err := oldLeader.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	if oldStatus.Role != raft.Leader ||
		oldStatus.Term != before.Term {
		t.Fatalf(
			"expected stale leader in old term: %+v",
			oldStatus,
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		300*time.Millisecond,
	)
	defer cancel()

	value, found, err :=
		oldLeader.shard.GetLinearizable(ctx, "alpha")

	if !errors.Is(err, context.DeadlineExceeded) ||
		found ||
		value != "" {
		t.Fatalf(
			"stale leader returned value=%q found=%v err=%v",
			value,
			found,
			err,
		)
	}

	t.Logf(
		"Old leader: term=%d local=100 linearizable read timed out",
		oldStatus.Term,
	)

	t.Logf(
		"New leader: term=%d linearizable value=200",
		after.Term,
	)

	t.Log("PASS: Stale leader could not serve stale data.")
}

func TestLiveReadIndexFailoverConcurrentReads(
	t *testing.T,
) {
	c, oldLeader := newLiveReadIndexLeader(t)

	next := replaceLiveReadIndexLeader(
		t, c, oldLeader, 2,
	)

	putLiveQuorumValue(
		t, c, next, "alpha", "200",
	)

	runLiveConcurrentReads(
		t,
		next,
		64,
		func(_ int) (string, string) {
			return "alpha", "200"
		},
	)

	requireLiveFormerLeaderRejection(
		t, oldLeader,
	)

	t.Log("PASS: Concurrent reads succeeded after failover.")
}
