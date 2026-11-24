package shard

import (
	"github.com/KrishShah1912/shardraft/internal/raft"
)

// StatusSnapshot returns Raft status and the applied index
// from the same synchronized shard state.
func (s *Shard) StatusSnapshot() (raft.Status, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return raft.Status{}, 0, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return raft.Status{}, 0, ErrStopped
	}

	return s.node.Status(), s.store.AppliedIndex(), nil
}
