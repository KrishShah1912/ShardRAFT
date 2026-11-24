package shard

import (
	"context"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestShardLinearizableIdentityExactConfirmation(
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
			"unexpected read: %q %v %v",
			value, found, err,
		)
	}
}

func TestShardLinearizableIdentityModifiedRequestID(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	state.RequestID++

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableIdentityModifiedIndex(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	if state.Index <= 1 {
		t.Fatal("expected confirmation index above one")
	}

	// This index is still committed and applied,
	// but it is not the accepted confirmation index.
	state.Index--

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableIdentityMissingConfirmation(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	s.mu.Lock()
	request.confirmation = nil
	s.mu.Unlock()

	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableIdentityNilRequest(
	t *testing.T,
) {
	s, _, state :=
		newLinearizableSafetyFixture(t)

	requireLinearizableSafetyError(
		t, s, nil, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableIdentityDuplicateAfterDrain(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	// The fixture already consumed the first notification.
	for i := 0; i < 100; i++ {
		deliverTestShardReadState(t, s, state)
	}

	requireNoShardReadNotification(t, request)
}

func TestShardLinearizableIdentityConflictingDuplicate(
	t *testing.T,
) {
	s, request, state :=
		newLinearizableSafetyFixture(t)

	conflict := state
	conflict.RequestID++

	deliverTestShardReadState(t, s, conflict)

	requireNoShardReadNotification(t, request)

	requireLinearizableSafetyError(
		t, s, request, conflict,
		ErrInvalidReadConfirmation,
	)

	value, found, err := s.finishLinearizableRead(
		context.Background(),
		"alpha",
		request,
		state,
	)

	if err != nil || !found || value != "100" {
		t.Fatalf(
			"original confirmation rejected: %q %v %v",
			value, found, err,
		)
	}
}

func TestShardLinearizableIdentityContextIsolation(
	t *testing.T,
) {
	s, request, state :=
		newShardReadStateFixture(t)

	original := append(
		[]byte(nil),
		state.Context...,
	)

	deliverTestShardReadState(t, s, state)

	// Mutating the original callback input must not
	// modify the stored or delivered confirmation.
	state.Context[0] ^= 0xff

	s.mu.Lock()

	accepted := request.confirmation

	if accepted == nil {
		s.mu.Unlock()
		t.Fatal("missing accepted confirmation")
	}

	if string(accepted.Context) != string(original) {
		s.mu.Unlock()
		t.Fatal("accepted context was mutated")
	}

	s.mu.Unlock()

	select {
	case result := <-request.done:
		if result.err != nil {
			t.Fatal(result.err)
		}

		if string(result.state.Context) != string(original) {
			t.Fatal("delivered context was mutated")
		}

		// The delivered result must also be independent
		// of the internally recorded confirmation.
		result.state.Context[0] ^= 0xff

	default:
		t.Fatal("missing read notification")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if string(request.confirmation.Context) !=
		string(original) {
		t.Fatal("notification modified accepted context")
	}
}

func TestShardLinearizableIdentityUnconfirmedRequest(
	t *testing.T,
) {
	s, request, state :=
		newShardReadStateFixture(t)

	// The request exists, but no callback has delivered
	// a confirmation for it.
	requireLinearizableSafetyError(
		t, s, request, state,
		ErrInvalidReadConfirmation,
	)
}

func TestShardLinearizableIdentityInvalidThenValid(
	t *testing.T,
) {
	s, request, state :=
		newShardReadStateFixture(t)

	invalid := state
	invalid.RequestID = 0

	deliverTestShardReadState(t, s, invalid)

	requireNoShardReadNotification(t, request)

	s.mu.Lock()

	if request.confirmation != nil {
		s.mu.Unlock()
		t.Fatal("invalid confirmation was recorded")
	}

	s.mu.Unlock()

	deliverTestShardReadState(t, s, state)

	requireShardReadNotification(
		t, request, state,
	)
}

func TestShardLinearizableIdentityIndependentRequests(
	t *testing.T,
) {
	s, first, firstState :=
		newShardReadStateFixture(t)

	second := registerMutationLifecycleRead(
		t, s, firstState.Term,
	)

	secondState := raft.ReadState{
		RequestID: firstState.RequestID + 1,
		Term:      firstState.Term,
		Index:     firstState.Index,
		Context: append(
			[]byte(nil),
			second.context...,
		),
	}

	deliverTestShardReadState(t, s, firstState)
	deliverTestShardReadState(t, s, secondState)

	requireShardReadNotification(
		t, first, firstState,
	)

	requireShardReadNotification(
		t, second, secondState,
	)

	s.mu.Lock()
	defer s.mu.Unlock()

	if first.confirmation == nil ||
		second.confirmation == nil {
		t.Fatal("missing accepted confirmations")
	}

	if first.confirmation.RequestID ==
		second.confirmation.RequestID {
		t.Fatal("confirmation identities collided")
	}

	if string(first.confirmation.Context) ==
		string(second.confirmation.Context) {
		t.Fatal("confirmation contexts collided")
	}
}

func TestShardLinearizableIdentityRepeatedRealReads(
	t *testing.T,
) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(
		t, s, "alpha", "100",
	)

	for i := 0; i < 100; i++ {
		requireLinearizableTestValue(
			t, s, "alpha", "100", true,
		)

		requireLinearizableNoPending(t, s)
	}
}
