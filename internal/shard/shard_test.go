package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func openTestShard(t *testing.T, path string) *Shard {
	t.Helper()

	s, err := Open(Config{
		ID:              1,
		Peers:           nil,
		ElectionTimeout: 5,
		WALPath:         path,
		Send: func(messages []raft.Message) error {
			if len(messages) != 0 {
				return errors.New("unexpected network messages")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func electTestLeader(t *testing.T, s *Shard) {
	t.Helper()

	for i := 0; i < 30; i++ {
		status, err := s.Status()
		if err != nil {
			t.Fatal(err)
		}
		if status.Role == raft.Leader {
			return
		}

		if err := s.Tick(); err != nil {
			t.Fatal(err)
		}
	}

	t.Fatal("single-node shard did not elect a leader")
}

func TestShardWriteSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	s := openTestShard(t, path)
	electTestLeader(t, s)

	command := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "user:1",
		Value:     "alice",
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	if err := s.Propose(command); err != nil {
		t.Fatal(err)
	}

	value, exists, err := s.Get("user:1")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "alice" {
		t.Fatalf("unexpected value: %q, exists=%v", value, exists)
	}

	before, err := s.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if before == 0 {
		t.Fatal("no committed entries were applied")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := openTestShard(t, path)
	defer restarted.Close()

	value, exists, err = restarted.Get("user:1")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "alice" {
		t.Fatalf(
			"recovered value = %q, exists=%v",
			value, exists,
		)
	}

	after, err := restarted.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf(
			"applied index after restart = %d, want %d",
			after, before,
		)
	}
}

func TestShardRejectsProposalBeforeElection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)
	defer s.Close()

	err := s.Propose(kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   "key",
		Value: "value",
	})
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestShardRejectsOperationsAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.Tick(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Tick: expected ErrClosed, got %v", err)
	}

	if err := s.Step(raft.Message{
		From: 2,
		To:   1,
	}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Step: expected ErrClosed, got %v", err)
	}

	if _, _, err := s.Get("key"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get: expected ErrClosed, got %v", err)
	}

	if _, err := s.Status(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Status: expected ErrClosed, got %v", err)
	}
}

func TestShardRejectsInvalidConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	_, err := Open(Config{
		ID:              1,
		Peers:           []raft.NodeID{1},
		ElectionTimeout: 5,
		WALPath:         path,
		Send: func([]raft.Message) error {
			return nil
		},
	})
	if err == nil {
		t.Fatal("expected duplicate node ID rejection")
	}
}
