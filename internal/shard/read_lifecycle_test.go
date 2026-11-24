package shard

import (
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func newLifecyclePendingRead(
	t *testing.T,
	s *Shard,
	term uint64,
) *pendingShardRead {
	t.Helper()

	request, err := s.registerPendingRead(term)
	if err != nil {
		t.Fatal(err)
	}

	return request
}

func requireLifecycleReadError(
	t *testing.T,
	request *pendingShardRead,
	expected error,
) {
	t.Helper()

	select {
	case result := <-request.done:
		if !errors.Is(result.err, expected) {
			t.Fatalf(
				"got error %v, want %v",
				result.err, expected,
			)
		}

		if result.state.RequestID != 0 {
			t.Fatal("terminal error contains confirmation")
		}

	default:
		t.Fatal("missing terminal notification")
	}
}

func requireLifecycleReadWaiting(
	t *testing.T,
	request *pendingShardRead,
) {
	t.Helper()

	select {
	case result := <-request.done:
		t.Fatalf(
			"unexpected notification: %+v",
			result,
		)

	default:
	}
}

func TestShardLinearizableLifecycleEmpty(t *testing.T) {
	s := &Shard{}

	s.reconcilePendingReads()

	if s.pendingReadCount() != 0 {
		t.Fatal("unexpected pending requests")
	}
}

func TestShardLinearizableLifecycleFailOne(t *testing.T) {
	s := &Shard{}

	first := newLifecyclePendingRead(t, s, 1)
	second := newLifecyclePendingRead(t, s, 1)

	s.failPendingRead(first, ErrClosed)

	requireLifecycleReadError(t, first, ErrClosed)
	requireLifecycleReadWaiting(t, second)

	if s.pendingReadCount() != 1 {
		t.Fatal("incorrect pending request count")
	}
}

func TestShardLinearizableLifecycleUnknownRequest(
	t *testing.T,
) {
	s := &Shard{}

	request := newLifecyclePendingRead(t, s, 1)

	s.removePendingRead(request)
	s.failPendingRead(request, ErrClosed)

	requireLifecycleReadWaiting(t, request)
}

func TestShardLinearizableLifecycleNilRequest(
	t *testing.T,
) {
	s := &Shard{}

	request := newLifecyclePendingRead(t, s, 1)

	s.failPendingRead(nil, ErrClosed)

	requireLifecycleReadWaiting(t, request)

	if s.pendingReadCount() != 1 {
		t.Fatal("valid request removed")
	}
}

func TestShardLinearizableLifecycleNilError(
	t *testing.T,
) {
	s := &Shard{}

	request := newLifecyclePendingRead(t, s, 1)

	s.failPendingRead(request, nil)

	requireLifecycleReadWaiting(t, request)

	if s.pendingReadCount() != 1 {
		t.Fatal("valid request removed")
	}
}

func TestShardLinearizableLifecycleFailureOverridesConfirmation(
	t *testing.T,
) {
	s := &Shard{}

	request := newLifecyclePendingRead(t, s, 1)

	request.done <- shardReadResult{
		state: raft.ReadState{
			RequestID: 1,
			Term:      1,
			Index:     1,
			Context:   request.context,
		},
	}

	s.failPendingRead(request, ErrStopped)

	requireLifecycleReadError(t, request, ErrStopped)

	if s.pendingReadCount() != 0 {
		t.Fatal("failed request retained")
	}
}

func TestShardLinearizableLifecycleFailureIdempotent(
	t *testing.T,
) {
	s := &Shard{}

	request := newLifecyclePendingRead(t, s, 1)

	s.failPendingRead(request, ErrClosed)
	s.failPendingRead(request, ErrStopped)

	requireLifecycleReadError(t, request, ErrClosed)
	requireLifecycleReadWaiting(t, request)
}

func TestShardLinearizableLifecycleFollowerInvalidatesAll(
	t *testing.T,
) {
	s := &Shard{}

	first := newLifecyclePendingRead(t, s, 1)
	second := newLifecyclePendingRead(t, s, 2)

	s.invalidateObsoleteReads(raft.Status{
		Role: raft.Follower,
		Term: 2,
	})

	requireLifecycleReadError(t, first, raft.ErrNotLeader)
	requireLifecycleReadError(t, second, raft.ErrNotLeader)

	if s.pendingReadCount() != 0 {
		t.Fatal("follower retained pending reads")
	}
}

func TestShardLinearizableLifecycleCandidateInvalidatesAll(
	t *testing.T,
) {
	s := &Shard{}

	request := newLifecyclePendingRead(t, s, 2)

	s.invalidateObsoleteReads(raft.Status{
		Role: raft.Candidate,
		Term: 2,
	})

	requireLifecycleReadError(
		t, request, raft.ErrNotLeader,
	)
}

func TestShardLinearizableLifecycleCurrentTermPreserved(
	t *testing.T,
) {
	s := &Shard{}

	first := newLifecyclePendingRead(t, s, 3)
	second := newLifecyclePendingRead(t, s, 3)

	s.invalidateObsoleteReads(raft.Status{
		Role: raft.Leader,
		Term: 3,
	})

	requireLifecycleReadWaiting(t, first)
	requireLifecycleReadWaiting(t, second)

	if s.pendingReadCount() != 2 {
		t.Fatal("current-term reads removed")
	}
}

func TestShardLinearizableLifecycleObsoleteTermsRemoved(
	t *testing.T,
) {
	s := &Shard{}

	old := newLifecyclePendingRead(t, s, 1)
	current := newLifecyclePendingRead(t, s, 2)
	future := newLifecyclePendingRead(t, s, 3)

	s.invalidateObsoleteReads(raft.Status{
		Role: raft.Leader,
		Term: 2,
	})

	requireLifecycleReadError(t, old, raft.ErrNotLeader)
	requireLifecycleReadError(t, future, raft.ErrNotLeader)
	requireLifecycleReadWaiting(t, current)

	if s.pendingReadCount() != 1 {
		t.Fatal("incorrect pending request count")
	}
}

func TestShardLinearizableLifecycleZeroTerm(
	t *testing.T,
) {
	s := &Shard{}

	request := newLifecyclePendingRead(t, s, 1)

	s.invalidateObsoleteReads(raft.Status{
		Role: raft.Leader,
		Term: 0,
	})

	requireLifecycleReadError(
		t, request, raft.ErrNotLeader,
	)
}

func TestShardLinearizableLifecycleClosed(t *testing.T) {
	s := &Shard{closed: true}

	request := newLifecyclePendingRead(t, s, 1)

	s.reconcilePendingReads()

	requireLifecycleReadError(t, request, ErrClosed)

	if s.pendingReadCount() != 0 {
		t.Fatal("closed shard retained reads")
	}
}

func TestShardLinearizableLifecycleStopped(t *testing.T) {
	s := &Shard{stopped: true}

	request := newLifecyclePendingRead(t, s, 1)

	s.reconcilePendingReads()

	requireLifecycleReadError(t, request, ErrStopped)

	if s.pendingReadCount() != 0 {
		t.Fatal("stopped shard retained reads")
	}
}

func TestShardLinearizableLifecycleClosedPrecedence(
	t *testing.T,
) {
	s := &Shard{
		closed:  true,
		stopped: true,
	}

	request := newLifecyclePendingRead(t, s, 1)

	s.reconcilePendingReads()

	requireLifecycleReadError(t, request, ErrClosed)
}

func TestShardLinearizableLifecycleActualLeader(
	t *testing.T,
) {
	s := newLinearizableReadFixture(t, true)

	s.mu.Lock()
	defer s.mu.Unlock()

	status := s.node.Status()

	request := newLifecyclePendingRead(
		t, s, status.Term,
	)

	s.reconcilePendingReads()

	requireLifecycleReadWaiting(t, request)

	if s.pendingReadCount() != 1 {
		t.Fatal("valid leader request removed")
	}

	s.removePendingRead(request)
}

func TestShardLinearizableLifecycleActualFollower(
	t *testing.T,
) {
	s := newLinearizableReadFixture(t, false)

	s.mu.Lock()
	defer s.mu.Unlock()

	request := newLifecyclePendingRead(t, s, 1)

	s.reconcilePendingReads()

	requireLifecycleReadError(
		t, request, raft.ErrNotLeader,
	)

	if s.pendingReadCount() != 0 {
		t.Fatal("follower retained pending request")
	}
}

func TestShardLinearizableLifecycleCapacityRecovery(
	t *testing.T,
) {
	s := &Shard{}

	requests := make(
		[]*pendingShardRead,
		0,
		maxPendingShardReads,
	)

	for i := 0; i < maxPendingShardReads; i++ {
		requests = append(
			requests,
			newLifecyclePendingRead(t, s, 1),
		)
	}

	s.invalidateObsoleteReads(raft.Status{
		Role: raft.Leader,
		Term: 2,
	})

	for _, request := range requests {
		requireLifecycleReadError(
			t, request, raft.ErrNotLeader,
		)
	}

	if s.pendingReadCount() != 0 {
		t.Fatal("capacity not released")
	}

	fresh := newLifecyclePendingRead(t, s, 2)

	if fresh.id <= requests[len(requests)-1].id {
		t.Fatal("request identity reused")
	}

	requireLifecycleReadWaiting(t, fresh)
}
