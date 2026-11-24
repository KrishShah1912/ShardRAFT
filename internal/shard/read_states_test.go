package shard

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func newShardReadStateFixture(
	t *testing.T,
) (*Shard, *pendingShardRead, raft.ReadState) {
	t.Helper()

	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)

	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})

	electTestLeader(t, s)

	s.mu.Lock()
	defer s.mu.Unlock()

	status := s.node.Status()

	request := registerTestShardRead(
		t,
		s,
		status.Term,
	)

	state := raft.ReadState{
		RequestID: 1,
		Term:      status.Term,
		Index:     status.Commit,
		Context:   append([]byte(nil), request.context...),
	}

	if state.Index == 0 {
		t.Fatal("missing committed leader entry")
	}

	return s, request, state
}

func deliverTestShardReadState(
	t *testing.T,
	s *Shard,
	state raft.ReadState,
) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.handleReadState(state); err != nil {
		t.Fatal(err)
	}
}

func requireShardReadNotification(
	t *testing.T,
	request *pendingShardRead,
	expected raft.ReadState,
) shardReadResult {
	t.Helper()

	select {
	case result := <-request.done:
		if result.err != nil {
			t.Fatal(result.err)
		}

		if result.state.RequestID != expected.RequestID ||
			result.state.Term != expected.Term ||
			result.state.Index != expected.Index ||
			!bytes.Equal(
				result.state.Context,
				expected.Context,
			) {
			t.Fatalf(
				"unexpected confirmation: %+v",
				result.state,
			)
		}

		return result

	default:
		t.Fatal("missing read confirmation")
		return shardReadResult{}
	}
}

func requireNoShardReadNotification(
	t *testing.T,
	request *pendingShardRead,
) {
	t.Helper()

	select {
	case result := <-request.done:
		t.Fatalf("unexpected notification: %+v", result)
	default:
	}
}

func TestShardReadStateValidConfirmation(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	deliverTestShardReadState(t, s, state)

	requireShardReadNotification(t, request, state)
}

func TestShardReadStateUnknownContext(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	state.Context = make([]byte, 16)

	deliverTestShardReadState(t, s, state)

	requireNoShardReadNotification(t, request)
}

func TestShardReadStateRemovedRequest(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	s.mu.Lock()
	removed := s.removePendingRead(request)
	s.mu.Unlock()

	if !removed {
		t.Fatal("request removal failed")
	}

	deliverTestShardReadState(t, s, state)

	requireNoShardReadNotification(t, request)
}

func TestShardReadStateMalformedIdentity(t *testing.T) {
	tests := []struct {
		name   string
		change func(*raft.ReadState)
	}{
		{
			name: "zero-request-id",
			change: func(state *raft.ReadState) {
				state.RequestID = 0
			},
		},
		{
			name: "zero-term",
			change: func(state *raft.ReadState) {
				state.Term = 0
			},
		},
		{
			name: "zero-index",
			change: func(state *raft.ReadState) {
				state.Index = 0
			},
		},
		{
			name: "empty-context",
			change: func(state *raft.ReadState) {
				state.Context = nil
			},
		},
		{
			name: "short-context",
			change: func(state *raft.ReadState) {
				state.Context = []byte("invalid")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, request, state :=
				newShardReadStateFixture(t)

			test.change(&state)

			deliverTestShardReadState(t, s, state)

			requireNoShardReadNotification(t, request)
		})
	}
}

func TestShardReadStateWrongTerm(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	state.Term++

	deliverTestShardReadState(t, s, state)

	requireNoShardReadNotification(t, request)
}

func TestShardReadStateBeyondCommit(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	state.Index++

	deliverTestShardReadState(t, s, state)

	requireNoShardReadNotification(t, request)
}

func TestShardReadStateDuplicateNotification(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	for i := 0; i < 100; i++ {
		deliverTestShardReadState(t, s, state)
	}

	requireShardReadNotification(t, request, state)

	requireNoShardReadNotification(t, request)
}

func TestShardReadStateContextIsolation(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	expected := state
	expected.Context = append(
		[]byte(nil),
		state.Context...,
	)

	deliverTestShardReadState(t, s, state)

	state.Context[0] ^= 0xff

	requireShardReadNotification(
		t,
		request,
		expected,
	)
}

func TestShardReadStateIndependentRequests(t *testing.T) {
	s, first, firstState := newShardReadStateFixture(t)

	s.mu.Lock()

	second := registerTestShardRead(
		t,
		s,
		first.term,
	)

	s.mu.Unlock()

	secondState := firstState
	secondState.RequestID++
	secondState.Context = append(
		[]byte(nil),
		second.context...,
	)

	deliverTestShardReadState(t, s, secondState)

	requireNoShardReadNotification(t, first)

	requireShardReadNotification(
		t,
		second,
		secondState,
	)

	deliverTestShardReadState(t, s, firstState)

	requireShardReadNotification(
		t,
		first,
		firstState,
	)
}

func TestShardReadStateStoppedShard(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	deliverTestShardReadState(t, s, state)

	requireNoShardReadNotification(t, request)
}

func TestShardReadStateClosedShard(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing the shard must notify pending readers.
	requireLifecycleReadError(t, request, ErrClosed)

	// Closing the shard must release pending requests.
	s.mu.Lock()
	count := s.pendingReadCount()
	s.mu.Unlock()

	if count != 0 {
		t.Fatalf("closed shard retained %d pending reads", count)
	}

	// A late confirmation must not revive a closed request.
	deliverTestShardReadState(t, s, state)

	requireNoShardReadNotification(t, request)
}

func TestShardReadStatePreservesTerminalError(t *testing.T) {
	s, request, state := newShardReadStateFixture(t)

	request.done <- shardReadResult{
		err: ErrStopped,
	}

	deliverTestShardReadState(t, s, state)

	result := <-request.done

	if result.err != ErrStopped {
		t.Fatalf(
			"terminal error was replaced: %v",
			result.err,
		)
	}

	requireNoShardReadNotification(t, request)
}

func TestShardReadStateActualDriverIntegration(t *testing.T) {
	s, request, _ := newShardReadStateFixture(t)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.node.ReadIndex(
		request.context,
	); err != nil {
		t.Fatal(err)
	}

	ready := s.node.Ready()

	if len(ready.ReadStates) != 1 {
		t.Fatalf(
			"expected one confirmation, got %d",
			len(ready.ReadStates),
		)
	}

	expected := ready.ReadStates[0]

	if err := s.driver.Process(); err != nil {
		t.Fatal(err)
	}

	requireShardReadNotification(
		t,
		request,
		expected,
	)

	if len(s.node.Ready().ReadStates) != 0 {
		t.Fatal("confirmation was not acknowledged")
	}

	for i := 0; i < 10; i++ {
		if err := s.driver.Process(); err != nil {
			t.Fatal(err)
		}
	}

	requireNoShardReadNotification(t, request)
}

func TestShardReadStateActualMultipleConfirmations(t *testing.T) {
	s, first, _ := newShardReadStateFixture(t)

	s.mu.Lock()
	defer s.mu.Unlock()

	requests := []*pendingShardRead{first}

	for i := 0; i < 9; i++ {
		requests = append(
			requests,
			registerTestShardRead(t, s, first.term),
		)
	}

	for _, request := range requests {
		if err := s.node.ReadIndex(
			request.context,
		); err != nil {
			t.Fatal(err)
		}
	}

	states := s.node.Ready().ReadStates

	if len(states) != len(requests) {
		t.Fatalf(
			"confirmations=%d requests=%d",
			len(states),
			len(requests),
		)
	}

	if err := s.driver.Process(); err != nil {
		t.Fatal(err)
	}

	for i, request := range requests {
		requireShardReadNotification(
			t,
			request,
			states[i],
		)

		requireNoShardReadNotification(t, request)
	}

	if len(s.node.Ready().ReadStates) != 0 {
		t.Fatal("unacknowledged confirmations remain")
	}
}

func TestShardReadStateActualUnknownConfirmation(t *testing.T) {
	s, request, _ := newShardReadStateFixture(t)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.node.ReadIndex(
		[]byte("unknown-context"),
	); err != nil {
		t.Fatal(err)
	}

	if err := s.driver.Process(); err != nil {
		t.Fatal(err)
	}

	requireNoShardReadNotification(t, request)

	if len(s.node.Ready().ReadStates) != 0 {
		t.Fatal("obsolete confirmation was retained")
	}

	if s.driver.Stopped() {
		t.Fatal("unknown context stopped the driver")
	}
}
