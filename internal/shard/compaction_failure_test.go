package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
)

func TestStoppedShardRejectsAllOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)
	defer s.Close()

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	if err := s.Tick(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Tick error = %v, want ErrStopped", err)
	}

	if err := s.Propose(kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   "key",
		Value: "value",
	}); !errors.Is(err, ErrStopped) {
		t.Fatalf("Propose error = %v, want ErrStopped", err)
	}

	if _, _, err := s.Get("key"); !errors.Is(err, ErrStopped) {
		t.Fatalf("Get error = %v, want ErrStopped", err)
	}

	if _, err := s.Status(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Status error = %v, want ErrStopped", err)
	}

	if _, err := s.AppliedIndex(); !errors.Is(err, ErrStopped) {
		t.Fatalf("AppliedIndex error = %v, want ErrStopped", err)
	}

	if err := s.Compact(path + ".snapshot"); !errors.Is(err, ErrStopped) {
		t.Fatalf("Compact error = %v, want ErrStopped", err)
	}
}

func TestInvalidCompactionPathDoesNotStopShard(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)
	defer s.Close()

	if err := s.Compact(path + ".wrong"); err == nil {
		t.Fatal("expected invalid compaction path error")
	}

	if err := s.Tick(); err != nil {
		t.Fatalf("shard should remain usable: %v", err)
	}
}

func TestShardCanCloseAfterStoppedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	if err := s.Close(); err != nil {
		t.Fatalf("Close failed on stopped shard: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("repeated Close failed: %v", err)
	}
}
