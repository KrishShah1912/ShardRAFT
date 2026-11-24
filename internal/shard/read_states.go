package shard

import (
	"github.com/KrishShah1912/shardraft/internal/raft"
)

// configureReadStates connects the driver to the shard's
// volatile read tracking and actual KV application index.
//
// Called during Open, before the shard is published.
func (s *Shard) configureReadStates() error {
	if err := s.driver.SetReadStateAppliedIndex(
		s.store.AppliedIndex,
	); err != nil {
		return err
	}

	return s.driver.SetReadStateHandler(
		s.handleReadState,
	)
}

// handleReadState receives an internal Raft confirmation.
//
// The caller already holds s.mu during normal operation.
// This callback must never acquire s.mu or block.
//
// A delivered confirmation is not itself a completed
// client read. The waiting caller must revalidate state
// before accessing the KV store.
func (s *Shard) handleReadState(
	state raft.ReadState,
) error {
	if s.closed || s.stopped || s.driver.Stopped() {
		return nil
	}

	if state.RequestID == 0 ||
		state.Term == 0 ||
		state.Index == 0 ||
		len(state.Context) != 16 {
		return nil
	}

	request := s.pendingReads[string(state.Context)]

	if request == nil {
		// The request may have been canceled or removed.
		return nil
	}

	if request.term != state.Term {
		return nil
	}

	status := s.node.Status()

	if status.Role != raft.Leader ||
		status.Term != state.Term ||
		status.Commit < state.Index {
		return nil
	}

	if s.store.AppliedIndex() < state.Index {
		return nil
	}

	// Preserve ownership of the confirmation context.
	if request.confirmation != nil {
		return nil
	}

	accepted := state
	accepted.Context = append([]byte(nil), state.Context...)
	request.confirmation = &accepted

	confirmation := state
	confirmation.Context = append(
		[]byte(nil),
		state.Context...,
	)

	// A duplicate notification must not block or replace
	// an existing confirmation or terminal error.
	select {
	case request.done <- shardReadResult{
		state: confirmation,
	}:
	default:
	}

	return nil
}
