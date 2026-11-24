package shard

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type concurrentCancelRead struct {
	cancel context.CancelFunc
	done   <-chan lifecycleAsyncReadResult
}

func startConcurrentCancelBatch(
	t *testing.T,
	s *Shard,
	count int,
) []concurrentCancelRead {
	t.Helper()

	batch := make([]concurrentCancelRead, count)

	for i := range batch {
		ctx, cancel := context.WithCancel(
			context.Background(),
		)

		t.Cleanup(cancel)

		batch[i] = concurrentCancelRead{
			cancel: cancel,
			done: startLifecycleAsyncRead(
				ctx, s,
			),
		}
	}

	waitLifecyclePendingReads(t, s, count)

	return batch
}

func cancelConcurrentBatch(
	batch []concurrentCancelRead,
) {
	var wg sync.WaitGroup

	for _, request := range batch {
		wg.Add(1)

		go func(cancel context.CancelFunc) {
			defer wg.Done()
			cancel()
		}(request.cancel)
	}

	wg.Wait()
}

func requireConcurrentCancelSuccess(
	t *testing.T,
	result lifecycleAsyncReadResult,
) {
	t.Helper()

	if result.err != nil ||
		!result.found ||
		result.value != "100" {
		t.Fatalf(
			"unexpected read result: %+v",
			result,
		)
	}
}

func snapshotConcurrentCancelRequests(
	s *Shard,
) []*pendingShardRead {
	s.mu.Lock()
	defer s.mu.Unlock()

	requests := make(
		[]*pendingShardRead,
		0,
		len(s.pendingReads),
	)

	for _, request := range s.pendingReads {
		requests = append(requests, request)
	}

	return requests
}

func TestShardLinearizableCancelConcurrentAll(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	batch := startConcurrentCancelBatch(
		t, leader, 64,
	)

	requests := snapshotConcurrentCancelRequests(
		leader,
	)

	cancelConcurrentBatch(batch)

	for _, request := range batch {
		requireLifecycleAsyncError(
			t, request.done,
			context.Canceled,
		)
	}

	requireDeadlineCapacityEmpty(t, leader)

	for _, request := range requests {
		requireShardCancelNoRaftReads(
			t, leader, request,
		)
	}

	c.deliver(t)

	for _, request := range requests {
		requireNoShardReadNotification(
			t, request,
		)
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableCancelConcurrentMixed(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	batch := startConcurrentCancelBatch(
		t, leader, 64,
	)

	cancelConcurrentBatch(batch[:32])

	for _, request := range batch[:32] {
		requireLifecycleAsyncError(
			t, request.done,
			context.Canceled,
		)
	}

	requireMutationLifecycleCount(
		t, leader, 32,
	)

	c.deliver(t)

	for _, request := range batch[32:] {
		requireConcurrentCancelSuccess(
			t,
			awaitLifecycleAsyncRead(
				t, request.done,
			),
		)
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableCancelConcurrentDeadlines(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		500*time.Millisecond,
	)
	defer cancel()

	results := make(
		[]<-chan lifecycleAsyncReadResult,
		32,
	)

	for i := range results {
		results[i] = startLifecycleAsyncRead(
			ctx, leader,
		)
	}

	waitLifecyclePendingReads(t, leader, 32)

	requests := snapshotConcurrentCancelRequests(
		leader,
	)

	for _, done := range results {
		requireLifecycleAsyncError(
			t, done,
			context.DeadlineExceeded,
		)
	}

	requireDeadlineCapacityEmpty(t, leader)

	for _, request := range requests {
		requireShardCancelNoRaftReads(
			t, leader, request,
		)
	}

	c.deliver(t)

	for _, request := range requests {
		requireNoShardReadNotification(
			t, request,
		)
	}
}

func TestShardLinearizableCancelConcurrentLateResponses(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	batch := startConcurrentCancelBatch(
		t, leader, 32,
	)

	requests := snapshotConcurrentCancelRequests(
		leader,
	)

	cancelConcurrentBatch(batch)

	for _, request := range batch {
		requireLifecycleAsyncError(
			t, request.done,
			context.Canceled,
		)
	}

	fresh := startConcurrentCancelBatch(
		t, leader, 32,
	)

	// Deliver both canceled and fresh read probes.
	c.deliver(t)

	for _, request := range fresh {
		requireConcurrentCancelSuccess(
			t,
			awaitLifecycleAsyncRead(
				t, request.done,
			),
		)
	}

	for _, request := range requests {
		requireNoShardReadNotification(
			t, request,
		)

		requireShardCancelNoRaftReads(
			t, leader, request,
		)
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableCancelConcurrentDuplicateResponses(
	t *testing.T,
) {
	c, leader, status := newLifecycleReadLeader(t)

	batch := startConcurrentCancelBatch(
		t, leader, 16,
	)

	requests := snapshotConcurrentCancelRequests(
		leader,
	)

	// Capture actual correlated heartbeat probes.
	c.mu.Lock()

	var probes []raft.Message

	for _, message := range c.messages {
		if message.Type == raft.MsgHeartbeat &&
			message.RequestID != 0 &&
			message.From == 1 {
			probes = append(probes, message)
		}
	}

	c.mu.Unlock()

	if len(probes) == 0 {
		t.Fatal("no correlated read probes captured")
	}

	cancelConcurrentBatch(batch)

	for _, request := range batch {
		requireLifecycleAsyncError(
			t, request.done,
			context.Canceled,
		)
	}

	for round := 0; round < 10; round++ {
		for _, probe := range probes {
			if err := leader.Step(raft.Message{
				Type:      raft.MsgHeartbeatResp,
				From:      probe.To,
				To:        probe.From,
				Term:      status.Term,
				RequestID: probe.RequestID,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	c.deliver(t)

	for _, request := range requests {
		requireNoShardReadNotification(
			t, request,
		)

		requireShardCancelNoRaftReads(
			t, leader, request,
		)
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableCancelConcurrentQuorumRace(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	for round := 0; round < 10; round++ {
		batch := startConcurrentCancelBatch(
			t, leader, 32,
		)

		start := make(chan struct{})
		finished := make(chan struct{})

		go func() {
			defer close(finished)

			<-start

			cancelConcurrentBatch(batch)
		}()

		close(start)

		c.deliver(t)

		<-finished

		for _, request := range batch {
			result := awaitLifecycleAsyncRead(
				t, request.done,
			)

			if result.err == nil {
				requireConcurrentCancelSuccess(
					t, result,
				)
				continue
			}

			if !errors.Is(
				result.err,
				context.Canceled,
			) ||
				result.found ||
				result.value != "" {
				t.Fatalf(
					"invalid race outcome: %+v",
					result,
				)
			}
		}

		requireDeadlineCapacityEmpty(
			t, leader,
		)
	}
}

func TestShardLinearizableCancelConcurrentCapacity(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	// 20 batches of 64 reads = 1,280 requests.
	// No quorum confirmations are delivered.
	for round := 0; round < 20; round++ {
		batch := startConcurrentCancelBatch(
			t, leader, 64,
		)

		cancelConcurrentBatch(batch)

		for _, request := range batch {
			requireLifecycleAsyncError(
				t, request.done,
				context.Canceled,
			)
		}

		requireDeadlineCapacityEmpty(
			t, leader,
		)
	}

	c.deliver(t)

	fresh := startConcurrentCancelBatch(
		t, leader, 64,
	)

	c.deliver(t)

	for _, request := range fresh {
		requireConcurrentCancelSuccess(
			t,
			awaitLifecycleAsyncRead(
				t, request.done,
			),
		)
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableCancelConcurrentLeadership(
	t *testing.T,
) {
	c, leader, status := newLifecycleReadLeader(t)

	batch := startConcurrentCancelBatch(
		t, leader, 64,
	)

	cancelConcurrentBatch(batch[:32])

	for _, request := range batch[:32] {
		requireLifecycleAsyncError(
			t, request.done,
			context.Canceled,
		)
	}

	if err := leader.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   1,
		Term: status.Term + 1,
	}); err != nil {
		t.Fatal(err)
	}

	for _, request := range batch[32:] {
		requireLifecycleAsyncError(
			t, request.done,
			raft.ErrNotLeader,
		)
	}

	c.deliver(t)

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableCancelConcurrentFreshReads(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	for round := 0; round < 10; round++ {
		canceled := startConcurrentCancelBatch(
			t, leader, 16,
		)

		cancelConcurrentBatch(canceled)

		for _, request := range canceled {
			requireLifecycleAsyncError(
				t, request.done,
				context.Canceled,
			)
		}

		fresh := startConcurrentCancelBatch(
			t, leader, 16,
		)

		c.deliver(t)

		for _, request := range fresh {
			requireConcurrentCancelSuccess(
				t,
				awaitLifecycleAsyncRead(
					t, request.done,
				),
			)
		}

		requireDeadlineCapacityEmpty(
			t, leader,
		)
	}
}

func TestShardLinearizableCancelConcurrentTicks(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	batch := startConcurrentCancelBatch(
		t, leader, 64,
	)

	requests := snapshotConcurrentCancelRequests(
		leader,
	)

	finished := make(chan struct{})

	go func() {
		defer close(finished)

		cancelConcurrentBatch(batch)
	}()

	for i := 0; i < 20; i++ {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}
	}

	<-finished

	for _, request := range batch {
		requireLifecycleAsyncError(
			t, request.done,
			context.Canceled,
		)
	}

	for _, request := range requests {
		requireShardCancelNoRaftReads(
			t, leader, request,
		)
	}

	c.deliver(t)

	for _, request := range requests {
		requireNoShardReadNotification(
			t, request,
		)
	}

	requireDeadlineCapacityEmpty(t, leader)
}
