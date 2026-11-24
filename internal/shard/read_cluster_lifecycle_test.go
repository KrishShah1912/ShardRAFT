package shard

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

type lifecycleReadCluster struct {
	nodes map[raft.NodeID]*Shard

	mu       sync.Mutex
	messages []raft.Message
}

func newLifecycleReadCluster(
	t *testing.T,
) *lifecycleReadCluster {
	t.Helper()

	c := &lifecycleReadCluster{
		nodes: make(map[raft.NodeID]*Shard),
	}

	root := t.TempDir()

	for id := raft.NodeID(1); id <= 3; id++ {
		peers := make([]raft.NodeID, 0, 2)

		for peer := raft.NodeID(1); peer <= 3; peer++ {
			if peer != id {
				peers = append(peers, peer)
			}
		}

		s, err := Open(Config{
			ID:              id,
			Peers:           peers,
			ElectionTimeout: 10 + int(id)*10,
			WALPath: filepath.Join(
				root,
				fmt.Sprintf("node%d.wal", id),
			),
			Send: c.send,
		})

		if err != nil {
			t.Fatal(err)
		}

		c.nodes[id] = s

		t.Cleanup(func() {
			_ = s.Close()
		})
	}

	return c
}

func (c *lifecycleReadCluster) send(
	messages []raft.Message,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, msg := range messages {
		copyMsg := msg

		copyMsg.Entries = append(
			[]raft.Entry(nil),
			msg.Entries...,
		)

		for i := range copyMsg.Entries {
			copyMsg.Entries[i].Data = append(
				[]byte(nil),
				msg.Entries[i].Data...,
			)
		}

		c.messages = append(c.messages, copyMsg)
	}

	return nil
}

func (c *lifecycleReadCluster) deliver(
	t *testing.T,
) {
	t.Helper()

	for round := 0; round < 1000; round++ {
		c.mu.Lock()

		messages := c.messages
		c.messages = nil

		c.mu.Unlock()

		if len(messages) == 0 {
			return
		}

		for _, msg := range messages {
			node := c.nodes[msg.To]

			if node == nil {
				t.Fatalf(
					"unknown destination: %d",
					msg.To,
				)
			}

			if err := node.Step(msg); err != nil {
				t.Fatalf(
					"deliver %d -> %d: %v",
					msg.From, msg.To, err,
				)
			}
		}
	}

	t.Fatal("message delivery did not converge")
}

func newLifecycleReadLeader(
	t *testing.T,
) (*lifecycleReadCluster, *Shard, raft.Status) {
	t.Helper()

	c := newLifecycleReadCluster(t)
	leader := c.nodes[1]

	// Tick only node 1 during the initial election.
	// Votes and replication use actual Raft messages.
	for i := 0; i < 200; i++ {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}

		c.deliver(t)

		status, err := leader.Status()
		if err != nil {
			t.Fatal(err)
		}

		if status.Role != raft.Leader ||
			status.Commit == 0 {
			continue
		}

		err = leader.Propose(kvstore.Command{
			Op:    "PUT",
			Key:   "alpha",
			Value: "100",
		})

		if err != nil {
			t.Fatal(err)
		}

		c.deliver(t)

		value, found, err := leader.Get("alpha")

		if err != nil || !found || value != "100" {
			t.Fatalf(
				"initial write failed: %q %v %v",
				value, found, err,
			)
		}

		status, err = leader.Status()
		if err != nil {
			t.Fatal(err)
		}

		return c, leader, status
	}

	t.Fatal("failed to elect and initialize leader")

	return nil, nil, raft.Status{}
}

type lifecycleAsyncReadResult struct {
	value string
	found bool
	err   error
}

func startLifecycleAsyncRead(
	ctx context.Context,
	s *Shard,
) <-chan lifecycleAsyncReadResult {
	done := make(chan lifecycleAsyncReadResult, 1)

	go func() {
		value, found, err :=
			s.GetLinearizable(ctx, "alpha")

		done <- lifecycleAsyncReadResult{
			value: value,
			found: found,
			err:   err,
		}
	}()

	return done
}

func waitLifecyclePendingReads(
	t *testing.T,
	s *Shard,
	expected int,
) {
	t.Helper()

	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		s.mu.Lock()
		count := s.pendingReadCount()
		s.mu.Unlock()

		if count == expected {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf(
		"pending reads did not reach %d",
		expected,
	)
}

func awaitLifecycleAsyncRead(
	t *testing.T,
	done <-chan lifecycleAsyncReadResult,
) lifecycleAsyncReadResult {
	t.Helper()

	select {
	case result := <-done:
		return result

	case <-time.After(2 * time.Second):
		t.Fatal("waiting reader was not notified")

		return lifecycleAsyncReadResult{}
	}
}

func requireLifecycleAsyncError(
	t *testing.T,
	done <-chan lifecycleAsyncReadResult,
	expected error,
) {
	t.Helper()

	result := awaitLifecycleAsyncRead(t, done)

	if result.value != "" ||
		result.found ||
		!errors.Is(result.err, expected) {
		t.Fatalf(
			"unexpected result: %+v; want %v",
			result, expected,
		)
	}
}

func TestShardLinearizableClusterLifecycleQuorumSuccess(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	// No quorum responses have been delivered yet.
	select {
	case result := <-done:
		t.Fatalf(
			"read completed before quorum: %+v",
			result,
		)
	default:
	}

	c.deliver(t)

	result := awaitLifecycleAsyncRead(t, done)

	if result.err != nil ||
		!result.found ||
		result.value != "100" {
		t.Fatalf("unexpected read: %+v", result)
	}

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableClusterLifecycleHigherTerm(
	t *testing.T,
) {
	_, leader, status := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	err := leader.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   1,
		Term: status.Term + 1,
	})

	if err != nil {
		t.Fatal(err)
	}

	after, err := leader.Status()
	if err != nil {
		t.Fatal(err)
	}

	if after.Role != raft.Follower ||
		after.Term != status.Term+1 {
		t.Fatalf(
			"expected higher-term follower: %+v",
			after,
		)
	}

	requireLifecycleAsyncError(
		t, done, raft.ErrNotLeader,
	)

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableClusterLifecycleSameTerm(
	t *testing.T,
) {
	_, leader, status := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	// Inject a same-term heartbeat from a configured peer
	// to exercise defensive leadership invalidation.
	err := leader.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   1,
		Term: status.Term,
	})

	if err != nil {
		t.Fatal(err)
	}

	after, err := leader.Status()
	if err != nil {
		t.Fatal(err)
	}

	if after.Role != raft.Follower ||
		after.Term != status.Term {
		t.Fatalf(
			"expected same-term follower: %+v",
			after,
		)
	}

	requireLifecycleAsyncError(
		t, done, raft.ErrNotLeader,
	)

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableClusterLifecycleCloseWaiting(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}

	requireLifecycleAsyncError(t, done, ErrClosed)

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableClusterLifecycleStopWaiting(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	leader.mu.Lock()

	err := leader.stop(
		errors.New("test terminal failure"),
	)

	leader.mu.Unlock()

	if !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}

	requireLifecycleAsyncError(t, done, ErrStopped)

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableClusterLifecycleMultipleWaiting(
	t *testing.T,
) {
	_, leader, status := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	const readers = 32

	results := make(
		[]<-chan lifecycleAsyncReadResult,
		readers,
	)

	for i := range results {
		results[i] = startLifecycleAsyncRead(
			ctx, leader,
		)
	}

	waitLifecyclePendingReads(
		t, leader, readers,
	)

	err := leader.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   1,
		Term: status.Term + 1,
	})

	if err != nil {
		t.Fatal(err)
	}

	for _, done := range results {
		requireLifecycleAsyncError(
			t, done, raft.ErrNotLeader,
		)
	}

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableClusterLifecycleConcurrentQuorum(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	const readers = 32

	results := make(
		[]<-chan lifecycleAsyncReadResult,
		readers,
	)

	for i := range results {
		results[i] = startLifecycleAsyncRead(
			ctx, leader,
		)
	}

	waitLifecyclePendingReads(
		t, leader, readers,
	)

	c.deliver(t)

	for _, done := range results {
		result := awaitLifecycleAsyncRead(t, done)

		if result.err != nil ||
			!result.found ||
			result.value != "100" {
			t.Fatalf(
				"unexpected read: %+v",
				result,
			)
		}
	}

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableClusterLifecycleCancelWaiting(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	cancel()

	requireLifecycleAsyncError(
		t, done, context.Canceled,
	)

	requireMutationLifecycleCount(t, leader, 0)

	// Deliver delayed quorum responses. They must not
	// recreate the canceled shard request.
	c.deliver(t)

	requireMutationLifecycleCount(t, leader, 0)

	fresh := startLifecycleAsyncRead(
		context.Background(), leader,
	)

	waitLifecyclePendingReads(t, leader, 1)

	c.deliver(t)

	result := awaitLifecycleAsyncRead(t, fresh)

	if result.err != nil ||
		!result.found ||
		result.value != "100" {
		t.Fatalf(
			"fresh read failed: %+v",
			result,
		)
	}
}

func TestShardLinearizableClusterLifecycleDeadline(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	// Intentionally withhold all quorum responses.
	requireLifecycleAsyncError(
		t, done, context.DeadlineExceeded,
	)

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableClusterLifecycleDriverFailure(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	// Only the isolated temporary test WAL is closed.
	leader.mu.Lock()

	if err := leader.storage.Close(); err != nil {
		leader.mu.Unlock()
		t.Fatal(err)
	}

	err := leader.node.Propose(
		[]byte("test-driver-failure"),
	)

	if err == nil {
		err = leader.processDriver()
	}

	stopped := leader.stopped
	driverStopped := leader.driver.Stopped()

	leader.closed = true

	leader.mu.Unlock()

	if err == nil || !stopped || !driverStopped {
		t.Fatal("expected terminal driver failure")
	}

	requireLifecycleAsyncError(
		t, done, ErrStopped,
	)

	requireMutationLifecycleCount(t, leader, 0)
}
