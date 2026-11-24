package shard

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func requireShardCancelNoRaftReads(
	t *testing.T,
	s *Shard,
	request *pendingShardRead,
) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.node.CancelReadIndex(
		request.term,
		request.context,
	) {
		t.Fatal("Raft read remained after shard cleanup")
	}
}

func getShardCancelRequest(
	t *testing.T,
	s *Shard,
) *pendingShardRead {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pendingReads) != 1 {
		t.Fatalf(
			"expected one pending read, got %d",
			len(s.pendingReads),
		)
	}

	for _, request := range s.pendingReads {
		return request
	}

	t.Fatal("missing pending read")

	return nil
}

func TestShardLinearizableCancelIntegrationHelper(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	request, err := leader.beginLinearizableRead(
		context.Background(),
	)

	if err != nil {
		t.Fatal(err)
	}

	leader.mu.Lock()

	removed := leader.cancelLinearizableRead(request)

	leader.mu.Unlock()

	if !removed {
		t.Fatal("expected successful cancellation")
	}

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelIntegrationNil(
	t *testing.T,
) {
	s := newLinearizableReadFixture(t, true)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancelLinearizableRead(nil) {
		t.Fatal("nil cancellation succeeded")
	}
}

func TestShardLinearizableCancelIntegrationIdempotent(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	request, err := leader.beginLinearizableRead(
		context.Background(),
	)

	if err != nil {
		t.Fatal(err)
	}

	leader.mu.Lock()
	defer leader.mu.Unlock()

	if !leader.cancelLinearizableRead(request) {
		t.Fatal("initial cancellation failed")
	}

	for i := 0; i < 100; i++ {
		if leader.cancelLinearizableRead(request) {
			t.Fatal("repeated cancellation succeeded")
		}
	}
}

func TestShardLinearizableCancelIntegrationContext(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	request := getShardCancelRequest(t, leader)

	cancel()

	requireLifecycleAsyncError(
		t, done, context.Canceled,
	)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelIntegrationDeadline(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		500*time.Millisecond,
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	request := getShardCancelRequest(t, leader)

	requireLifecycleAsyncError(
		t, done, context.DeadlineExceeded,
	)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelIntegrationSuccessfulRead(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	done := startLifecycleAsyncRead(
		context.Background(),
		leader,
	)

	waitLifecyclePendingReads(t, leader, 1)

	request := getShardCancelRequest(t, leader)

	c.deliver(t)

	result := awaitLifecycleAsyncRead(t, done)

	if result.err != nil ||
		!result.found ||
		result.value != "100" {
		t.Fatalf("unexpected read: %+v", result)
	}

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelIntegrationLateResponse(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := startLifecycleAsyncRead(ctx, leader)

	waitLifecyclePendingReads(t, leader, 1)

	request := getShardCancelRequest(t, leader)

	cancel()

	requireLifecycleAsyncError(
		t, done, context.Canceled,
	)

	// Deliver probes already handed to the transport.
	// Their responses must not recreate canceled reads.
	c.deliver(t)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)

	requireNoShardReadNotification(t, request)

	fresh := startLifecycleAsyncRead(
		context.Background(),
		leader,
	)

	waitLifecyclePendingReads(t, leader, 1)

	c.deliver(t)

	result := awaitLifecycleAsyncRead(t, fresh)

	if result.err != nil ||
		!result.found ||
		result.value != "100" {
		t.Fatalf("fresh read failed: %+v", result)
	}
}

func TestShardLinearizableCancelIntegrationIndependent(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	firstCtx, firstCancel := context.WithCancel(
		context.Background(),
	)
	defer firstCancel()

	first := startLifecycleAsyncRead(
		firstCtx, leader,
	)

	waitLifecyclePendingReads(t, leader, 1)

	request := getShardCancelRequest(t, leader)

	second := startLifecycleAsyncRead(
		context.Background(), leader,
	)

	waitLifecyclePendingReads(t, leader, 2)

	firstCancel()

	requireLifecycleAsyncError(
		t, first, context.Canceled,
	)

	requireMutationLifecycleCount(t, leader, 1)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)

	c.deliver(t)

	result := awaitLifecycleAsyncRead(t, second)

	if result.err != nil ||
		!result.found ||
		result.value != "100" {
		t.Fatalf(
			"independent read failed: %+v",
			result,
		)
	}

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableCancelIntegrationAfterConfirmation(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	cancel()

	value, found, err := s.finishLinearizableRead(
		ctx, "alpha", request, state,
	)

	if value != "" ||
		found ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf(
			"unexpected result: %q %v %v",
			value, found, err,
		)
	}

	s.mu.Lock()

	s.cancelLinearizableRead(request)

	s.mu.Unlock()

	requireMutationLifecycleCount(t, s, 0)

	requireShardCancelNoRaftReads(
		t, s, request,
	)
}

func TestShardLinearizableCancelIntegrationLeadershipLoss(
	t *testing.T,
) {
	_, leader, status := newLifecycleReadLeader(t)

	done := startLifecycleAsyncRead(
		context.Background(),
		leader,
	)

	waitLifecyclePendingReads(t, leader, 1)

	request := getShardCancelRequest(t, leader)

	if err := leader.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   1,
		Term: status.Term + 1,
	}); err != nil {
		t.Fatal(err)
	}

	requireLifecycleAsyncError(
		t, done, raft.ErrNotLeader,
	)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelIntegrationNoLogGrowth(
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

	for i := 0; i < 100; i++ {
		request, err := leader.beginLinearizableRead(
			context.Background(),
		)

		if err != nil {
			t.Fatal(err)
		}

		leader.mu.Lock()

		removed := leader.cancelLinearizableRead(
			request,
		)

		leader.mu.Unlock()

		if !removed {
			t.Fatalf("cancellation failed at %d", i)
		}
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
		t.Fatal("cancellation changed consensus progress")
	}

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableCancelIntegrationCapacity(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	// Exceed the Raft pending-read limit without
	// delivering any quorum confirmations.
	for i := 0; i < 1100; i++ {
		request, err := leader.beginLinearizableRead(
			context.Background(),
		)

		if err != nil {
			t.Fatalf(
				"read %d failed: %v",
				i, err,
			)
		}

		leader.mu.Lock()

		removed := leader.cancelLinearizableRead(
			request,
		)

		leader.mu.Unlock()

		if !removed {
			t.Fatalf(
				"cancellation failed at %d",
				i,
			)
		}
	}

	requireMutationLifecycleCount(t, leader, 0)
}
