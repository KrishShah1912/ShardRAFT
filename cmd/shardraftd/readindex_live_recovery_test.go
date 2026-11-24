package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
)

func stopLiveReadIndexInbound(
	t *testing.T,
	node *liveReadIndexNode,
) string {
	t.Helper()

	address := node.listener.Addr().String()

	node.server.Stop()
	_ = node.listener.Close()

	select {
	case <-node.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("node %d server did not stop", node.id)
	}

	t.Logf(
		"Node %d inbound gRPC stopped: %s",
		node.id,
		address,
	)

	return address
}

func restoreLiveReadIndexInbound(
	t *testing.T,
	node *liveReadIndexNode,
	address string,
) {
	t.Helper()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}

	var peers []raft.NodeID

	for id := raft.NodeID(1); id <= 3; id++ {
		if id != node.id {
			peers = append(peers, id)
		}
	}

	server := grpc.NewServer()

	shardraftpb.RegisterRaftServiceServer(
		server,
		newRaftGRPCServer(1, node.shard, peers),
	)

	done := make(chan struct{})

	node.listener = listener
	node.server = server
	node.done = done

	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()

	t.Logf(
		"Node %d inbound gRPC restored: %s",
		node.id,
		address,
	)
}

func isolateLiveReadIndexFollowers(
	t *testing.T,
	c *liveReadIndexCluster,
) (string, string) {
	t.Helper()

	address2 := stopLiveReadIndexInbound(t, c.nodes[2])
	address3 := stopLiveReadIndexInbound(t, c.nodes[3])

	return address2, address3
}

func requireLiveReadIndexUnavailable(
	t *testing.T,
	leader *liveReadIndexNode,
	timeout time.Duration,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		timeout,
	)
	defer cancel()

	start := time.Now()

	value, found, err :=
		leader.shard.GetLinearizable(ctx, "alpha")

	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) ||
		found ||
		value != "" {
		t.Fatalf(
			"unavailable read: value=%q found=%v err=%v",
			value,
			found,
			err,
		)
	}

	if elapsed < timeout || elapsed > timeout+2*time.Second {
		t.Fatalf(
			"unexpected deadline duration: %v; expected %v",
			elapsed,
			timeout,
		)
	}

	t.Logf(
		"Read correctly timed out after %v",
		elapsed,
	)
}

func waitLiveReadIndexRecovery(
	t *testing.T,
	leader *liveReadIndexNode,
	expected string,
) {
	t.Helper()

	waitLiveReadIndexCondition(
		t,
		"linearizable read after quorum restoration",
		func() bool {
			if err := leader.shard.Tick(); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(
				context.Background(),
				500*time.Millisecond,
			)

			value, found, err :=
				leader.shard.GetLinearizable(ctx, "alpha")

			cancel()

			if errors.Is(err, context.DeadlineExceeded) {
				return false
			}

			if err != nil {
				t.Fatalf("recovery read failed: %v", err)
			}

			if !found || value != expected {
				t.Fatalf(
					"recovery value=%q found=%v; want %q",
					value,
					found,
					expected,
				)
			}

			return true
		},
	)

	t.Logf(
		"PASS: Restored quorum returned alpha=%s",
		expected,
	)
}

func TestLiveReadIndexRecoveryOneFollowerUnavailable(
	t *testing.T,
) {
	c, leader := newLiveReadIndexLeader(t)

	stopLiveReadIndexInbound(t, c.nodes[3])

	for i := 0; i < 20; i++ {
		requireLiveQuorumValue(
			t, leader, "alpha", "100", true,
		)
	}

	t.Log("PASS: Majority remained available with one follower down.")
}

func TestLiveReadIndexRecoveryQuorumUnavailable(
	t *testing.T,
) {
	c, leader := newLiveReadIndexLeader(t)

	isolateLiveReadIndexFollowers(t, c)

	local, found, err := leader.shard.Get("alpha")

	if err != nil || !found || local != "100" {
		t.Fatalf(
			"unexpected local value=%q found=%v err=%v",
			local,
			found,
			err,
		)
	}

	requireLiveReadIndexUnavailable(
		t, leader, 200*time.Millisecond,
	)

	st, err := leader.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	if st.Role != raft.Leader {
		t.Fatal("expected isolated node to retain leader role")
	}

	t.Log("PASS: Leader role alone did not permit a linearizable read.")
}

func TestLiveReadIndexRecoveryDeadline(
	t *testing.T,
) {
	c, leader := newLiveReadIndexLeader(t)

	isolateLiveReadIndexFollowers(t, c)

	for _, timeout := range []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
	} {
		requireLiveReadIndexUnavailable(
			t, leader, timeout,
		)
	}

	t.Log("PASS: Caller deadlines enforced during quorum loss.")
}

func TestLiveReadIndexRecoveryCancellation(
	t *testing.T,
) {
	c, leader := newLiveReadIndexLeader(t)

	address2, address3 :=
		isolateLiveReadIndexFollowers(t, c)

	type result struct {
		value string
		found bool
		err   error
	}

	before := leader.transport.Stats().Sent

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := make(chan result, 1)

	go func() {
		value, found, err :=
			leader.shard.GetLinearizable(ctx, "alpha")

		done <- result{
			value: value,
			found: found,
			err:   err,
		}
	}()

	waitLiveReadIndexCondition(
		t,
		"outstanding read sends quorum probes",
		func() bool {
			return leader.transport.Stats().Sent > before
		},
	)

	cancel()

	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) ||
			got.found ||
			got.value != "" {
			t.Fatalf("unexpected canceled read: %+v", got)
		}

	case <-time.After(time.Second):
		t.Fatal("canceled read did not return")
	}

	restoreLiveReadIndexInbound(t, c.nodes[2], address2)
	restoreLiveReadIndexInbound(t, c.nodes[3], address3)

	waitLiveReadIndexRecovery(t, leader, "100")

	t.Log("PASS: Outstanding read canceled; fresh read succeeded.")
}

func TestLiveReadIndexRecoveryRestoreQuorum(
	t *testing.T,
) {
	c, leader := newLiveReadIndexLeader(t)

	before, err := leader.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	address2, address3 :=
		isolateLiveReadIndexFollowers(t, c)

	requireLiveReadIndexUnavailable(
		t, leader, 100*time.Millisecond,
	)

	// Restore only one follower. Two of three nodes
	// must be sufficient to establish a fresh quorum.
	restoreLiveReadIndexInbound(t, c.nodes[2], address2)

	waitLiveReadIndexRecovery(t, leader, "100")

	after, err := leader.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	if after.Role != raft.Leader ||
		after.Term != before.Term {
		t.Fatalf(
			"unexpected leadership change: before=%+v after=%+v",
			before,
			after,
		)
	}

	restoreLiveReadIndexInbound(t, c.nodes[3], address3)

	putLiveQuorumValue(
		t, c, leader, "alpha", "200",
	)

	requireLiveQuorumValue(
		t, leader, "alpha", "200", true,
	)

	t.Log("PASS: Majority restored without a new election.")
}

func TestLiveReadIndexRecoveryRepeatedIsolation(
	t *testing.T,
) {
	c, leader := newLiveReadIndexLeader(t)

	for _, expected := range []string{
		"200",
		"300",
		"400",
	} {
		address2, address3 :=
			isolateLiveReadIndexFollowers(t, c)

		requireLiveReadIndexUnavailable(
			t, leader, 100*time.Millisecond,
		)

		restoreLiveReadIndexInbound(
			t, c.nodes[2], address2,
		)

		restoreLiveReadIndexInbound(
			t, c.nodes[3], address3,
		)

		putLiveQuorumValue(
			t, c, leader, "alpha", expected,
		)

		waitLiveReadIndexRecovery(
			t, leader, expected,
		)
	}

	t.Log("PASS: Three isolation and recovery cycles completed.")
}

func TestLiveReadIndexRecoveryNoLogGrowth(
	t *testing.T,
) {
	c, leader := newLiveReadIndexLeader(t)

	before, beforeApplied, err :=
		leader.shard.StatusSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	address2, address3 :=
		isolateLiveReadIndexFollowers(t, c)

	for i := 0; i < 10; i++ {
		requireLiveReadIndexUnavailable(
			t, leader, 50*time.Millisecond,
		)
	}

	restoreLiveReadIndexInbound(t, c.nodes[2], address2)
	restoreLiveReadIndexInbound(t, c.nodes[3], address3)

	waitLiveReadIndexRecovery(t, leader, "100")

	after, afterApplied, err :=
		leader.shard.StatusSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	if before != after ||
		beforeApplied != afterApplied {
		t.Fatalf(
			"read-only state changed: before=%+v after=%+v applied=%d->%d",
			before,
			after,
			beforeApplied,
			afterApplied,
		)
	}

	t.Logf(
		"PASS: Term=%d commit=%d applied=%d unchanged.",
		after.Term,
		after.Commit,
		afterApplied,
	)
}
