package shard

import "testing"

func TestShardLinearizableCancelNilNodeRegistered(
	t *testing.T,
) {
	s := &Shard{}

	request := registerTestShardRead(t, s, 1)

	if !s.cancelLinearizableRead(request) {
		t.Fatal("registered request was not removed")
	}

	if s.pendingReadCount() != 0 {
		t.Fatal("pending request remains")
	}
}

func TestShardLinearizableCancelNilNodeUnknown(
	t *testing.T,
) {
	s := &Shard{}
	other := &Shard{}

	request := registerTestShardRead(t, other, 1)

	if s.cancelLinearizableRead(request) {
		t.Fatal("unknown request cancellation succeeded")
	}

	if other.pendingReadCount() != 1 {
		t.Fatal("unrelated shard request changed")
	}
}

func TestShardLinearizableCancelNilNodeRepeated(
	t *testing.T,
) {
	s := &Shard{}

	request := registerTestShardRead(t, s, 1)

	if !s.cancelLinearizableRead(request) {
		t.Fatal("initial cancellation failed")
	}

	for i := 0; i < 100; i++ {
		if s.cancelLinearizableRead(request) {
			t.Fatal("repeated cancellation succeeded")
		}
	}
}

func TestShardLinearizableCancelNilNodeBulk(
	t *testing.T,
) {
	s := &Shard{}

	requests := make([]*pendingShardRead, 100)

	for i := range requests {
		requests[i] = registerTestShardRead(
			t, s, 1,
		)
	}

	s.failPendingReads(ErrStopped)

	if s.pendingReadCount() != 0 {
		t.Fatal("bulk cleanup retained requests")
	}

	for _, request := range requests {
		requireLifecycleReadError(
			t, request, ErrStopped,
		)
	}
}
