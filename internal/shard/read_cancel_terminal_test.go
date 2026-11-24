package shard

import (
	"context"
	"fmt"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func beginTerminalCancelRead(
	t *testing.T,
	s *Shard,
) *pendingShardRead {
	t.Helper()

	request, err := s.beginLinearizableRead(
		context.Background(),
	)

	if err != nil {
		t.Fatal(err)
	}

	return request
}

func TestShardLinearizableCancelTerminalClose(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	request := beginTerminalCancelRead(t, leader)

	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadError(
		t, request, ErrClosed,
	)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelTerminalStop(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	request := beginTerminalCancelRead(t, leader)

	leader.mu.Lock()
	leader.stop(fmt.Errorf("test terminal stop"))
	leader.mu.Unlock()

	requireLifecycleReadError(
		t, request, ErrStopped,
	)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelTerminalCloseMultiple(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	requests := make([]*pendingShardRead, 100)

	for i := range requests {
		requests[i] = beginTerminalCancelRead(
			t, leader,
		)
	}

	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}

	for _, request := range requests {
		requireLifecycleReadError(
			t, request, ErrClosed,
		)

		requireShardCancelNoRaftReads(
			t, leader, request,
		)
	}

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableCancelTerminalStopMultiple(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	requests := make([]*pendingShardRead, 100)

	for i := range requests {
		requests[i] = beginTerminalCancelRead(
			t, leader,
		)
	}

	leader.mu.Lock()
	leader.stop(fmt.Errorf("test terminal stop"))
	leader.mu.Unlock()

	for _, request := range requests {
		requireLifecycleReadError(
			t, request, ErrStopped,
		)

		requireShardCancelNoRaftReads(
			t, leader, request,
		)
	}

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableCancelTerminalIndividual(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	first := beginTerminalCancelRead(t, leader)
	second := beginTerminalCancelRead(t, leader)

	leader.mu.Lock()
	leader.failPendingRead(first, ErrStopped)
	leader.mu.Unlock()

	requireLifecycleReadError(
		t, first, ErrStopped,
	)

	requireShardCancelNoRaftReads(
		t, leader, first,
	)

	requireMutationLifecycleCount(t, leader, 1)

	c.deliver(t)

	select {
	case result := <-second.done:
		if result.err != nil ||
			result.state.RequestID == 0 {
			t.Fatalf(
				"independent read failed: %+v",
				result,
			)
		}

	default:
		t.Fatal("independent read was not confirmed")
	}

	leader.mu.Lock()
	leader.cancelLinearizableRead(second)
	leader.mu.Unlock()

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableCancelTerminalBulk(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	requests := make([]*pendingShardRead, 32)

	for i := range requests {
		requests[i] = beginTerminalCancelRead(
			t, leader,
		)
	}

	leader.mu.Lock()
	leader.failPendingReads(ErrStopped)
	leader.mu.Unlock()

	for _, request := range requests {
		requireLifecycleReadError(
			t, request, ErrStopped,
		)

		requireShardCancelNoRaftReads(
			t, leader, request,
		)
	}

	requireMutationLifecycleCount(t, leader, 0)
}

func TestShardLinearizableCancelTerminalRepeated(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	request := beginTerminalCancelRead(t, leader)

	leader.mu.Lock()

	leader.failPendingReads(ErrStopped)

	for i := 0; i < 100; i++ {
		leader.failPendingReads(ErrStopped)
		leader.failPendingRead(request, ErrStopped)
	}

	leader.mu.Unlock()

	requireLifecycleReadError(
		t, request, ErrStopped,
	)

	requireNoShardReadNotification(t, request)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelTerminalLeadership(
	t *testing.T,
) {
	_, leader, status := newLifecycleReadLeader(t)

	request := beginTerminalCancelRead(t, leader)

	if err := leader.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   1,
		Term: status.Term + 1,
	}); err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadError(
		t, request, raft.ErrNotLeader,
	)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelTerminalDriverFailure(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	request := beginTerminalCancelRead(t, leader)

	leader.mu.Lock()

	if err := leader.storage.Close(); err != nil {
		leader.mu.Unlock()
		t.Fatal(err)
	}

	// Storage is already closed. Prevent fixture cleanup
	// from closing the same storage a second time.
	defer func() {
		leader.mu.Lock()
		leader.closed = true
		leader.mu.Unlock()
	}()

	if err := leader.node.Propose(
		[]byte("terminal-cancel-test"),
	); err != nil {
		leader.mu.Unlock()
		t.Fatal(err)
	}

	err := leader.processDriver()

	stopped := leader.stopped &&
		leader.driver.Stopped()

	leader.mu.Unlock()

	if err == nil || !stopped {
		t.Fatal("expected terminal driver failure")
	}

	requireLifecycleReadError(
		t, request, ErrStopped,
	)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelTerminalCompleted(
	t *testing.T,
) {
	s := newLinearizableReadFixture(t, true)

	s.mu.Lock()

	status := s.node.Status()

	request, err := s.registerPendingRead(
		status.Term,
	)

	if err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}

	if err := s.node.ReadIndex(
		request.context,
	); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}

	if len(s.node.Ready().ReadStates) != 1 {
		s.mu.Unlock()
		t.Fatal("expected unconsumed confirmation")
	}

	s.failPendingReads(ErrStopped)

	remaining := len(s.node.Ready().ReadStates)

	s.mu.Unlock()

	if remaining != 0 {
		t.Fatal("terminal cleanup retained confirmation")
	}

	requireLifecycleReadError(
		t, request, ErrStopped,
	)

	requireMutationLifecycleCount(t, s, 0)

	requireShardCancelNoRaftReads(
		t, s, request,
	)
}

func TestShardLinearizableCancelTerminalLateResponse(
	t *testing.T,
) {
	c, leader, _ := newLifecycleReadLeader(t)

	request := beginTerminalCancelRead(t, leader)

	leader.mu.Lock()
	leader.failPendingReads(ErrStopped)
	leader.mu.Unlock()

	requireLifecycleReadError(
		t, request, ErrStopped,
	)

	c.deliver(t)

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)

	requireNoShardReadNotification(t, request)
}

func TestShardLinearizableCancelTerminalCapacity(
	t *testing.T,
) {
	_, leader, _ := newLifecycleReadLeader(t)

	// Exercise bulk lifecycle cleanup beyond the
	// maximum outstanding Raft read capacity.
	for batch := 0; batch < 12; batch++ {
		requests := make([]*pendingShardRead, 100)

		for i := range requests {
			requests[i] = beginTerminalCancelRead(
				t, leader,
			)
		}

		leader.mu.Lock()
		leader.failPendingReads(ErrStopped)
		leader.mu.Unlock()

		for _, request := range requests {
			requireLifecycleReadError(
				t, request, ErrStopped,
			)
		}

		requireMutationLifecycleCount(t, leader, 0)
	}

	request := beginTerminalCancelRead(t, leader)

	leader.mu.Lock()
	leader.cancelLinearizableRead(request)
	leader.mu.Unlock()

	requireMutationLifecycleCount(t, leader, 0)
}
