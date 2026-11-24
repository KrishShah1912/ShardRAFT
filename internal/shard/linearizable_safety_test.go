package shard

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func newLinearizableSafetyFixture(
	t *testing.T,
) (*Shard, *pendingShardRead, raft.ReadState) {
	t.Helper()

	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "alpha", "100")

	request, err := s.beginLinearizableRead(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-request.done:
		if result.err != nil {
			t.Fatal(result.err)
		}

		t.Cleanup(func() {
			s.mu.Lock()
			defer s.mu.Unlock()

			s.removePendingRead(request)
		})

		return s, request, result.state

	default:
		t.Fatal("missing actual Raft confirmation")
		return nil, nil, raft.ReadState{}
	}
}

func requireLinearizableSafetyError(
	t *testing.T,
	s *Shard,
	request *pendingShardRead,
	state raft.ReadState,
	expected error,
) {
	t.Helper()

	value, found, err := s.finishLinearizableRead(
		context.Background(),
		"alpha",
		request,
		state,
	)

	if value != "" || found {
		t.Fatalf(
			"unsafe read returned data: %q %v",
			value, found,
		)
	}

	if !errors.Is(err, expected) {
		t.Fatalf(
			"unexpected error: got %v, want %v",
			err, expected,
		)
	}
}

func TestShardLinearizableSafetyValidConfirmation(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	value, found, err := s.finishLinearizableRead(
		context.Background(),
		"alpha",
		request,
		state,
	)

	if err != nil || !found || value != "100" {
		t.Fatalf(
			"unexpected result: %q %v %v",
			value, found, err,
		)
	}
}

func TestShardLinearizableSafetyZeroRequestID(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	state.RequestID = 0

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableSafetyZeroIndex(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	state.Index = 0

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableSafetyWrongTerm(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	state.Term++

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableSafetyWrongContext(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	state.Context = append(
		[]byte(nil),
		state.Context...,
	)

	state.Context[0] ^= 0xff

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableSafetyBeyondCommit(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	s.mu.Lock()
	state.Index = s.node.Status().Commit + 1
	s.mu.Unlock()

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableSafetyRemovedRequest(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	s.mu.Lock()
	removed := s.removePendingRead(request)
	s.mu.Unlock()

	if !removed {
		t.Fatal("request removal failed")
	}

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableSafetyDifferentRequest(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	s.mu.Lock()

	other := registerTestShardRead(
		t, s, request.term,
	)

	s.mu.Unlock()

	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.removePendingRead(other)
	})

	requireLinearizableSafetyError(
		t, s, other, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableSafetyObsoleteRequestTerm(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	s.mu.Lock()
	request.term++
	s.mu.Unlock()

	requireLinearizableSafetyError(
		t, s, request, state,
		raft.ErrNotLeader,
	)
}

func TestShardLinearizableSafetyClosedAfterConfirmation(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrClosed,
	)
}

func TestShardLinearizableSafetyStoppedAfterConfirmation(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrStopped,
	)
}

func TestShardLinearizableSafetyCanceledAfterConfirmation(
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

	if value != "" || found ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf(
			"unexpected result: %q %v %v",
			value, found, err,
		)
	}
}

func TestShardLinearizableSafetyExpiredAfterConfirmation(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	value, found, err := s.finishLinearizableRead(
		ctx, "alpha", request, state,
	)

	if value != "" || found ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf(
			"unexpected result: %q %v %v",
			value, found, err,
		)
	}
}

func TestShardLinearizableSafetyAppliedIndexEqual(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	s.mu.Lock()
	applied := s.store.AppliedIndex()
	s.mu.Unlock()

	if applied != state.Index {
		t.Fatalf(
			"applied=%d confirmation=%d",
			applied, state.Index,
		)
	}

	value, found, err := s.finishLinearizableRead(
		context.Background(),
		"alpha",
		request,
		state,
	)

	if err != nil || !found || value != "100" {
		t.Fatalf(
			"unexpected result: %q %v %v",
			value, found, err,
		)
	}
}

func TestShardLinearizableSafetyAppliedIndexAhead(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	putLinearizableTestValue(t, s, "alpha", "200")

	s.mu.Lock()
	applied := s.store.AppliedIndex()
	s.mu.Unlock()

	if applied <= state.Index {
		t.Fatal("application index did not advance")
	}

	value, found, err := s.finishLinearizableRead(
		context.Background(),
		"alpha",
		request,
		state,
	)

	if err != nil || !found || value != "200" {
		t.Fatalf(
			"unexpected result: %q %v %v",
			value, found, err,
		)
	}
}

func TestShardLinearizableSafetyApplicationBehind(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	// Test-only replacement of the state machine models
	// an application that has not reached the read barrier.
	s.mu.Lock()

	original := s.store
	s.store = kvstore.New()

	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.store = original
	}()

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}
