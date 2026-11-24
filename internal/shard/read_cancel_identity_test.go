package shard

import (
	"context"
	"testing"
)

func TestShardLinearizableCancelIdentityCopied(
	t *testing.T,
) {
	cluster, leader, _ := newLifecycleReadLeader(t)

	request, err := leader.beginLinearizableRead(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}

	copied := *request
	copied.context = append(
		[]byte(nil), request.context...,
	)

	leader.mu.Lock()
	canceled := leader.cancelLinearizableRead(&copied)
	leader.mu.Unlock()

	if canceled {
		t.Fatal("copied request canceled an active read")
	}

	requireMutationLifecycleCount(t, leader, 1)

	cluster.deliver(t)

	select {
	case result := <-request.done:
		if result.err != nil {
			t.Fatal(result.err)
		}

		value, found, err := leader.finishLinearizableRead(
			context.Background(),
			"alpha",
			request,
			result.state,
		)

		if err != nil || !found || value != "100" {
			t.Fatalf(
				"legitimate read failed: %q %v %v",
				value, found, err,
			)
		}

	default:
		t.Fatal("legitimate read lost its Raft confirmation")
	}

	leader.mu.Lock()
	leader.cancelLinearizableRead(request)
	leader.mu.Unlock()
}

func TestShardLinearizableCancelIdentityForeign(
	t *testing.T,
) {
	cluster, leader, _ := newLifecycleReadLeader(t)

	request, err := leader.beginLinearizableRead(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}

	other := &Shard{}

	foreign := registerTestShardRead(
		t, other, request.term,
	)

	// Deliberately reproduce the legitimate identity
	// while retaining a different request object.
	foreign.context = append(
		[]byte(nil), request.context...,
	)

	leader.mu.Lock()
	canceled := leader.cancelLinearizableRead(foreign)
	leader.mu.Unlock()

	if canceled {
		t.Fatal("foreign request canceled a local read")
	}

	requireMutationLifecycleCount(t, leader, 1)

	cluster.deliver(t)

	select {
	case result := <-request.done:
		if result.err != nil || result.state.RequestID == 0 {
			t.Fatalf("invalid confirmation: %+v", result)
		}
	default:
		t.Fatal("foreign request disrupted confirmation")
	}

	leader.mu.Lock()
	leader.cancelLinearizableRead(request)
	leader.mu.Unlock()
}

func TestShardLinearizableCancelIdentityUnknown(
	t *testing.T,
) {
	cluster, leader, _ := newLifecycleReadLeader(t)

	request, err := leader.beginLinearizableRead(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}

	unknown := &pendingShardRead{
		term:    request.term,
		context: []byte("unknown-read"),
		done:    make(chan shardReadResult, 1),
	}

	leader.mu.Lock()
	canceled := leader.cancelLinearizableRead(unknown)
	leader.mu.Unlock()

	if canceled {
		t.Fatal("unknown request cancellation succeeded")
	}

	requireMutationLifecycleCount(t, leader, 1)

	cluster.deliver(t)

	select {
	case result := <-request.done:
		if result.err != nil || result.state.RequestID == 0 {
			t.Fatalf("invalid confirmation: %+v", result)
		}
	default:
		t.Fatal("unknown request disrupted active read")
	}

	leader.mu.Lock()
	leader.cancelLinearizableRead(request)
	leader.mu.Unlock()
}

func TestShardLinearizableCancelIdentityRepeated(
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
			t.Fatal("obsolete request canceled twice")
		}
	}

	if leader.pendingReadCount() != 0 {
		t.Fatal("pending shard read remains")
	}

	if leader.node.CancelReadIndex(
		request.term, request.context,
	) {
		t.Fatal("pending Raft read remains")
	}
}

func TestShardLinearizableCancelIdentityOwned(
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
	canceled := leader.cancelLinearizableRead(request)
	leader.mu.Unlock()

	if !canceled {
		t.Fatal("owned request was not canceled")
	}

	requireMutationLifecycleCount(t, leader, 0)

	requireShardCancelNoRaftReads(
		t, leader, request,
	)
}

func TestShardLinearizableCancelIdentityTerminal(
	t *testing.T,
) {
	cluster, leader, _ := newLifecycleReadLeader(t)

	request, err := leader.beginLinearizableRead(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}

	copied := *request

	copied.context = append(
		[]byte(nil), request.context...,
	)

	copied.done = make(chan shardReadResult, 1)

	leader.mu.Lock()
	leader.failPendingRead(&copied, ErrStopped)
	leader.mu.Unlock()

	requireMutationLifecycleCount(t, leader, 1)
	requireLifecycleReadWaiting(t, &copied)
	requireLifecycleReadWaiting(t, request)

	cluster.deliver(t)

	select {
	case result := <-request.done:
		if result.err != nil || result.state.RequestID == 0 {
			t.Fatalf("invalid confirmation: %+v", result)
		}
	default:
		t.Fatal("foreign terminal failure disrupted active read")
	}

	leader.mu.Lock()
	leader.cancelLinearizableRead(request)
	leader.mu.Unlock()
}
