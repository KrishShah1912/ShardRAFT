package shard

import (
	"context"
	"errors"
	"testing"
	"time"
)

func requireDeadlineCapacityError(
	t *testing.T,
	s *Shard,
	ctx context.Context,
	expected error,
) {
	t.Helper()

	value, found, err := s.GetLinearizable(
		ctx, "alpha",
	)

	if value != "" ||
		found ||
		!errors.Is(err, expected) {
		t.Fatalf(
			"unexpected read: %q %v %v",
			value, found, err,
		)
	}
}

func requireDeadlineCapacityEmpty(
	t *testing.T,
	s *Shard,
) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pendingReadCount() != 0 {
		t.Fatal("shard pending reads remain")
	}

	if len(s.node.Ready().ReadStates) != 0 {
		t.Fatal("unconsumed confirmations remain")
	}
}

func TestShardLinearizableDeadlineCapacityDefault(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	start := time.Now()

	requireDeadlineCapacityError(
		t,
		leader,
		context.Background(),
		context.DeadlineExceeded,
	)

	elapsed := time.Since(start)

	if elapsed < defaultLinearizableReadTimeout {
		t.Fatalf("timeout occurred early: %v", elapsed)
	}

	if elapsed > defaultLinearizableReadTimeout+
		2*time.Second {
		t.Fatalf("timeout occurred late: %v", elapsed)
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityShorter(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()

	start := time.Now()

	requireDeadlineCapacityError(
		t, leader, ctx,
		context.DeadlineExceeded,
	)

	if time.Since(start) >=
		defaultLinearizableReadTimeout {
		t.Fatal("shorter client deadline ignored")
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityLonger(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	start := time.Now()

	requireDeadlineCapacityError(
		t, leader, ctx,
		context.DeadlineExceeded,
	)

	elapsed := time.Since(start)

	if elapsed < defaultLinearizableReadTimeout ||
		elapsed > defaultLinearizableReadTimeout+
			2*time.Second {
		t.Fatalf(
			"internal timeout not enforced: %v",
			elapsed,
		)
	}

	if ctx.Err() != nil {
		t.Fatal("parent context was unexpectedly canceled")
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityPreCanceled(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	leader.mu.Lock()
	before := leader.nextReadID
	leader.mu.Unlock()

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	cancel()

	for i := 0; i < 100; i++ {
		requireDeadlineCapacityError(
			t, leader, ctx,
			context.Canceled,
		)
	}

	leader.mu.Lock()
	after := leader.nextReadID
	leader.mu.Unlock()

	if before != after {
		t.Fatal("canceled contexts allocated read IDs")
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityPreExpired(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	leader.mu.Lock()
	before := leader.nextReadID
	leader.mu.Unlock()

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	for i := 0; i < 100; i++ {
		requireDeadlineCapacityError(
			t, leader, ctx,
			context.DeadlineExceeded,
		)
	}

	leader.mu.Lock()
	after := leader.nextReadID
	leader.mu.Unlock()

	if before != after {
		t.Fatal("expired contexts allocated read IDs")
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityRepeatedTimeout(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			20*time.Millisecond,
		)

		requireDeadlineCapacityError(
			t, leader, ctx,
			context.DeadlineExceeded,
		)

		cancel()

		requireDeadlineCapacityEmpty(t, leader)
	}
}

func TestShardLinearizableDeadlineCapacityRepeatedCancel(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(
			context.Background(),
		)

		done := startLifecycleAsyncRead(
			ctx, leader,
		)

		waitLifecyclePendingReads(t, leader, 1)

		request := getShardCancelRequest(t, leader)

		cancel()

		requireLifecycleAsyncError(
			t, done, context.Canceled,
		)

		requireShardCancelNoRaftReads(
			t, leader, request,
		)

		requireDeadlineCapacityEmpty(t, leader)
	}
}

func TestShardLinearizableDeadlineCapacityPublicStress(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	// Exceed the maximum Raft pending-read capacity
	// through the public API without delivering quorum.
	for i := 0; i < maxPendingShardReads+100; i++ {
		ctx, cancel := context.WithCancel(
			context.Background(),
		)

		done := startLifecycleAsyncRead(
			ctx, leader,
		)

		waitLifecyclePendingReads(t, leader, 1)

		cancel()

		requireLifecycleAsyncError(
			t, done, context.Canceled,
		)

		requireDeadlineCapacityEmpty(t, leader)
	}

	// Deliver previously transmitted probes and replies.
	c.deliver(t)

	requireDeadlineCapacityEmpty(t, leader)

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
			"read failed after capacity recovery: %+v",
			result,
		)
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityFull(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	requests := make(
		[]*pendingShardRead,
		maxPendingShardReads,
	)

	for i := range requests {
		request, err := leader.beginLinearizableRead(
			context.Background(),
		)

		if err != nil {
			t.Fatalf(
				"registration %d failed: %v",
				i, err,
			)
		}

		requests[i] = request
	}

	requireMutationLifecycleCount(
		t, leader, maxPendingShardReads,
	)

	requireDeadlineCapacityError(
		t,
		leader,
		context.Background(),
		ErrTooManyPendingReads,
	)

	leader.mu.Lock()

	for _, request := range requests {
		leader.cancelLinearizableRead(request)
	}

	leader.mu.Unlock()

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityRecoverOne(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	requests := make(
		[]*pendingShardRead,
		maxPendingShardReads,
	)

	for i := range requests {
		request, err := leader.beginLinearizableRead(
			context.Background(),
		)

		if err != nil {
			t.Fatal(err)
		}

		requests[i] = request
	}

	leader.mu.Lock()

	leader.cancelLinearizableRead(requests[0])

	leader.mu.Unlock()

	replacement, err := leader.beginLinearizableRead(
		context.Background(),
	)

	if err != nil {
		t.Fatalf("capacity not recovered: %v", err)
	}

	requireMutationLifecycleCount(
		t, leader, maxPendingShardReads,
	)

	leader.mu.Lock()

	for _, request := range requests {
		leader.cancelLinearizableRead(request)
	}

	leader.cancelLinearizableRead(replacement)

	leader.mu.Unlock()

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityRestoreQuorum(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	request := getShardCancelRequest(t, leader)

	requireLifecycleAsyncError(
		t, done, context.DeadlineExceeded,
	)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)

	c.deliver(t)

	requireNoShardReadNotification(t, request)

	for i := 0; i < 20; i++ {
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
				"recovered read %d failed: %+v",
				i, result,
			)
		}
	}

	requireDeadlineCapacityEmpty(t, leader)
}

func TestShardLinearizableDeadlineCapacityNoLogGrowth(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	before, err := leader.Status()
	if err != nil {
		t.Fatal(err)
	}

	appliedBefore, err := leader.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			20*time.Millisecond,
		)

		requireDeadlineCapacityError(
			t, leader, ctx,
			context.DeadlineExceeded,
		)

		cancel()
	}

	after, err := leader.Status()
	if err != nil {
		t.Fatal(err)
	}

	appliedAfter, err := leader.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}

	if before != after ||
		appliedBefore != appliedAfter {
		t.Fatal("expired reads changed consensus progress")
	}

	requireDeadlineCapacityEmpty(t, leader)
}
