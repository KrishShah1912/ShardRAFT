package shard

import (
	"context"
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestShardLinearizableTerminalLifecycleClose(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadError(t, request, ErrClosed)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleCloseMultiple(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	requests := make([]*pendingShardRead, 100)

	for i := range requests {
		requests[i] = registerMutationLifecycleRead(
			t, s, status.Term,
		)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for _, request := range requests {
		requireLifecycleReadError(
			t, request, ErrClosed,
		)
	}

	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleRepeatedClose(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	for i := 0; i < 10; i++ {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	requireLifecycleReadError(t, request, ErrClosed)
	requireLifecycleReadWaiting(t, request)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleCloseOverridesConfirmation(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	request.done <- shardReadResult{
		state: state,
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadError(t, request, ErrClosed)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleStop(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	s.mu.Lock()
	err := s.stop(errors.New("test terminal failure"))
	s.mu.Unlock()

	if !errors.Is(err, ErrStopped) {
		t.Fatalf("unexpected stop error: %v", err)
	}

	requireLifecycleReadError(t, request, ErrStopped)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleStopMultiple(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	requests := make([]*pendingShardRead, 100)

	for i := range requests {
		requests[i] = registerMutationLifecycleRead(
			t, s, status.Term,
		)
	}

	s.mu.Lock()
	err := s.stop(errors.New("test terminal failure"))
	s.mu.Unlock()

	if !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}

	for _, request := range requests {
		requireLifecycleReadError(
			t, request, ErrStopped,
		)
	}

	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleStopOverridesConfirmation(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	request.done <- shardReadResult{
		state: state,
	}

	s.mu.Lock()
	err := s.stop(errors.New("test terminal failure"))
	s.mu.Unlock()

	if !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}

	requireLifecycleReadError(t, request, ErrStopped)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleDriverSuccess(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	s.mu.Lock()
	err := s.processDriver()
	s.mu.Unlock()

	if err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadWaiting(t, request)
	requireMutationLifecycleCount(t, s, 1)
}

func TestShardLinearizableTerminalLifecycleActualDriverFailure(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	// Closing the temporary test WAL forces a real
	// storage failure during subsequent driver work.
	s.mu.Lock()

	if err := s.storage.Close(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}

	err := s.node.Propose([]byte("test-failure"))

	if err == nil {
		err = s.processDriver()
	}

	stopped := s.stopped
	driverStopped := s.driver.Stopped()

	// The WAL has already been closed by this test.
	s.closed = true

	s.mu.Unlock()

	if err == nil {
		t.Fatal("expected driver processing failure")
	}

	if !stopped || !driverStopped {
		t.Fatal("terminal driver failure not propagated")
	}

	requireLifecycleReadError(t, request, ErrStopped)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleCompaction(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	putLinearizableTestValue(
		t, s, "compact-key", "compact-value",
	)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	if err := s.Compact(s.snapshotPath); err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadWaiting(t, request)
	requireMutationLifecycleCount(t, s, 1)

	requireLinearizableTestValue(
		t, s, "compact-key", "compact-value", true,
	)
}

func TestShardLinearizableTerminalLifecycleClosedRead(
	t *testing.T,
) {
	s, _ := newMutationLifecycleLeader(t)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	value, found, err := s.GetLinearizable(
		context.Background(), "alpha",
	)

	if value != "" || found ||
		!errors.Is(err, ErrClosed) {
		t.Fatalf(
			"unexpected result: %q %v %v",
			value, found, err,
		)
	}

	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleStoppedRead(
	t *testing.T,
) {
	s, _ := newMutationLifecycleLeader(t)

	s.mu.Lock()
	err := s.stop(errors.New("test terminal failure"))
	s.mu.Unlock()

	if !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}

	value, found, err := s.GetLinearizable(
		context.Background(), "alpha",
	)

	if value != "" || found ||
		!errors.Is(err, ErrStopped) {
		t.Fatalf(
			"unexpected result: %q %v %v",
			value, found, err,
		)
	}

	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableTerminalLifecycleSuccessfulConfirmation(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.node.ReadIndex(
		request.context,
	); err != nil {
		t.Fatal(err)
	}

	states := s.node.Ready().ReadStates

	if len(states) != 1 {
		t.Fatal("expected one read confirmation")
	}

	if err := s.processDriver(); err != nil {
		t.Fatal(err)
	}

	requireShardReadNotification(
		t, request, states[0],
	)

	if s.pendingReadCount() != 1 {
		t.Fatal("valid confirmed request removed")
	}

	if len(s.node.Ready().ReadStates) != 0 {
		t.Fatal("confirmation was not acknowledged")
	}

	if s.node.Status().Role != raft.Leader {
		t.Fatal("leadership unexpectedly changed")
	}

	s.removePendingRead(request)
}
