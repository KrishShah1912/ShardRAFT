package shard

import (
	"github.com/KrishShah1912/shardraft/internal/raft"
)

// failPendingRead terminates one matching read request.
//
// A terminal error supersedes any queued confirmation.
// The caller must hold s.mu.
func (s *Shard) failPendingRead(
	request *pendingShardRead,
	err error,
) {
	if request == nil || err == nil {
		return
	}

	if s.pendingReads[string(request.context)] != request {
		return
	}

	s.cancelLinearizableRead(request)

	select {
	case <-request.done:
	default:
	}

	select {
	case request.done <- shardReadResult{err: err}:
	default:
	}
}

// invalidateObsoleteReads removes requests that no longer
// belong to the current leadership term.
//
// The caller must hold s.mu.
func (s *Shard) invalidateObsoleteReads(
	status raft.Status,
) {
	if status.Role != raft.Leader || status.Term == 0 {
		s.failPendingReads(raft.ErrNotLeader)
		return
	}

	for _, request := range s.pendingReads {
		if request.term != status.Term {
			s.failPendingRead(
				request,
				raft.ErrNotLeader,
			)
		}
	}
}

// reconcilePendingReads checks terminal shard state and
// current leadership before retaining pending requests.
//
// The caller must hold s.mu.
func (s *Shard) reconcilePendingReads() {
	if len(s.pendingReads) == 0 {
		return
	}

	if s.closed {
		s.failPendingReads(ErrClosed)
		return
	}

	if s.stopped || s.driver.Stopped() {
		s.failPendingReads(ErrStopped)
		return
	}

	s.invalidateObsoleteReads(s.node.Status())
}
