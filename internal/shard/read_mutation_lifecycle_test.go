package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func registerMutationLifecycleRead(
	t *testing.T,
	s *Shard,
	term uint64,
) *pendingShardRead {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	return newLifecyclePendingRead(t, s, term)
}

func requireMutationLifecycleCount(
	t *testing.T,
	s *Shard,
	expected int,
) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if actual := s.pendingReadCount(); actual != expected {
		t.Fatalf(
			"pending reads: got %d, want %d",
			actual, expected,
		)
	}
}

func newMutationLifecycleLeader(
	t *testing.T,
) (*Shard, raft.Status) {
	t.Helper()

	s := newLinearizableReadFixture(t, true)

	status, err := s.Status()
	if err != nil {
		t.Fatal(err)
	}

	if status.Role != raft.Leader {
		t.Fatal("expected leader")
	}

	return s, status
}

func TestShardLinearizableMutationLifecycleTickPreservesRead(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	for i := 0; i < 20; i++ {
		if err := s.Tick(); err != nil {
			t.Fatal(err)
		}
	}

	requireLifecycleReadWaiting(t, request)
	requireMutationLifecycleCount(t, s, 1)
}

func TestShardLinearizableMutationLifecycleTickRemovesObsoleteRead(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	obsolete := registerMutationLifecycleRead(
		t, s, status.Term+1,
	)

	current := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	if err := s.Tick(); err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadError(
		t, obsolete, raft.ErrNotLeader,
	)

	requireLifecycleReadWaiting(t, current)
	requireMutationLifecycleCount(t, s, 1)
}

func TestShardLinearizableMutationLifecycleFollowerTick(
	t *testing.T,
) {
	s, err := Open(Config{
		ID:              1,
		Peers:           []raft.NodeID{2, 3},
		ElectionTimeout: 10,
		WALPath: filepath.Join(
			t.TempDir(), "node.wal",
		),
		Send: func(messages []raft.Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	defer s.Close()

	request := registerMutationLifecycleRead(t, s, 1)

	if err := s.Tick(); err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadError(
		t, request, raft.ErrNotLeader,
	)

	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableMutationLifecycleProposePreservesRead(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	err := s.Propose(kvstore.Command{
		Op:    "PUT",
		Key:   "lifecycle-key",
		Value: "lifecycle-value",
	})
	if err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadWaiting(t, request)
	requireMutationLifecycleCount(t, s, 1)

	value, found, err := s.Get("lifecycle-key")

	if err != nil || !found ||
		value != "lifecycle-value" {
		t.Fatalf(
			"unexpected KV result: %q %v %v",
			value, found, err,
		)
	}
}

func TestShardLinearizableMutationLifecycleProposeRemovesObsoleteRead(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	obsolete := registerMutationLifecycleRead(
		t, s, status.Term+1,
	)

	current := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	err := s.Propose(kvstore.Command{
		Op:    "PUT",
		Key:   "obsolete-read-test",
		Value: "value",
	})
	if err != nil {
		t.Fatal(err)
	}

	requireLifecycleReadError(
		t, obsolete, raft.ErrNotLeader,
	)

	requireLifecycleReadWaiting(t, current)
	requireMutationLifecycleCount(t, s, 1)
}

func TestShardLinearizableMutationLifecycleFollowerPropose(
	t *testing.T,
) {
	s := newLinearizableReadFixture(t, false)

	request := registerMutationLifecycleRead(t, s, 1)

	err := s.Propose(kvstore.Command{
		Op:    "PUT",
		Key:   "follower-key",
		Value: "value",
	})

	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("unexpected proposal error: %v", err)
	}

	requireLifecycleReadError(
		t, request, raft.ErrNotLeader,
	)

	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableMutationLifecycleStepPreservesRead(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	// A message addressed to another node cannot
	// change this shard's leadership.
	_ = s.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   999,
		Term: status.Term,
	})

	after, err := s.Status()
	if err != nil {
		t.Fatal(err)
	}

	if after.Role != raft.Leader ||
		after.Term != status.Term {
		t.Fatal("leadership unexpectedly changed")
	}

	requireLifecycleReadWaiting(t, request)
	requireMutationLifecycleCount(t, s, 1)
}

func TestShardLinearizableMutationLifecycleStepRemovesObsoleteRead(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	obsolete := registerMutationLifecycleRead(
		t, s, status.Term+1,
	)

	current := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	_ = s.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   999,
		Term: status.Term,
	})

	requireLifecycleReadError(
		t, obsolete, raft.ErrNotLeader,
	)

	requireLifecycleReadWaiting(t, current)
	requireMutationLifecycleCount(t, s, 1)
}

func TestShardLinearizableMutationLifecycleStoppedTick(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	if err := s.Tick(); !errors.Is(err, ErrStopped) {
		t.Fatalf("unexpected tick error: %v", err)
	}

	requireLifecycleReadError(t, request, ErrStopped)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableMutationLifecycleStoppedStep(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	err := s.Step(raft.Message{})

	if !errors.Is(err, ErrStopped) {
		t.Fatalf("unexpected step error: %v", err)
	}

	requireLifecycleReadError(t, request, ErrStopped)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableMutationLifecycleStoppedPropose(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	request := registerMutationLifecycleRead(
		t, s, status.Term,
	)

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	err := s.Propose(kvstore.Command{
		Op:    "PUT",
		Key:   "stopped-key",
		Value: "value",
	})

	if !errors.Is(err, ErrStopped) {
		t.Fatalf("unexpected proposal error: %v", err)
	}

	requireLifecycleReadError(t, request, ErrStopped)
	requireMutationLifecycleCount(t, s, 0)
}

func TestShardLinearizableMutationLifecycleRepeatedOperations(
	t *testing.T,
) {
	s, status := newMutationLifecycleLeader(t)

	requests := make([]*pendingShardRead, 0, 100)

	for i := 0; i < 100; i++ {
		requests = append(
			requests,
			registerMutationLifecycleRead(
				t, s, status.Term,
			),
		)
	}

	for i := 0; i < 20; i++ {
		if err := s.Tick(); err != nil {
			t.Fatal(err)
		}

		err := s.Propose(kvstore.Command{
			Op:    "PUT",
			Key:   "repeated-key",
			Value: "value",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, request := range requests {
		requireLifecycleReadWaiting(t, request)
	}

	requireMutationLifecycleCount(t, s, 100)
}
