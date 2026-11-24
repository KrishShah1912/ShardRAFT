package shard

import (
	"errors"
	"os"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

type StorageDebug struct {
	Engine   string             `json:"engine"`
	WAL      wal.SegmentedStats `json:"wal"`
	Snapshot SnapshotDebug      `json:"snapshot"`
	Records  int                `json:"records"`
	Applied  uint64             `json:"applied_index"`
}

type SnapshotDebug struct {
	Exists       bool   `json:"exists"`
	Version      int    `json:"version,omitempty"`
	AppliedIndex uint64 `json:"applied_index,omitempty"`
	AppliedTerm  uint64 `json:"applied_term,omitempty"`
	Records      int    `json:"records,omitempty"`
	DedupRecords int    `json:"dedup_records,omitempty"`
	Valid        bool   `json:"valid"`
	Error        string `json:"error,omitempty"`
}

func (s *Shard) DebugRaft() (raft.DebugState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return raft.DebugState{}, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return raft.DebugState{}, ErrStopped
	}

	return s.node.DebugState(), nil
}

func (s *Shard) DebugRaftLog() (raft.DebugLog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return raft.DebugLog{}, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return raft.DebugLog{}, ErrStopped
	}

	return s.node.DebugLog(), nil
}

func (s *Shard) DebugKV() (map[string]string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, 0, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return nil, 0, ErrStopped
	}

	return s.store.Snapshot(), s.store.AppliedIndex(), nil
}

func (s *Shard) DebugStorage() (StorageDebug, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return StorageDebug{}, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return StorageDebug{}, ErrStopped
	}

	data := s.store.Snapshot()

	result := StorageDebug{
		Engine:  "pebble",
		WAL:     s.storage.Stats(),
		Records: len(data),
		Applied: s.store.AppliedIndex(),
	}

	saved, err := snapshot.Load(s.snapshotPath)

	switch {
	case err == nil:
		result.Snapshot = SnapshotDebug{
			Exists:       true,
			Version:      saved.Version,
			AppliedIndex: saved.AppliedIndex,
			AppliedTerm:  saved.AppliedTerm,
			Records:      len(saved.Data),
			DedupRecords: len(saved.Dedup),
			Valid:        true,
		}

	case errors.Is(err, os.ErrNotExist):
		result.Snapshot = SnapshotDebug{
			Exists: false,
			Valid:  true,
		}

	default:
		result.Snapshot = SnapshotDebug{
			Exists: true,
			Valid:  false,
			Error:  err.Error(),
		}
	}

	return result, nil
}
