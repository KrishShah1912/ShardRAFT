package raft

import "errors"

// ErrReadNotReady indicates that the leader has not
// committed an entry from its current leadership term.
//
// Callers may retry within their original deadline.
var ErrReadNotReady = errors.New(
	"current-term entry has not been committed",
)

// hasCommittedCurrentTerm reports whether the current
// leader has committed an entry from its own term.
//
// TermAt supports both retained log entries and the
// compacted snapshot boundary.
//
// This check does not establish a read quorum or
// guarantee that the state machine has applied the
// committed entry.
func (r *Raft) hasCommittedCurrentTerm() bool {
	if r.volatile.Role != Leader {
		return false
	}

	if r.hard.Term == 0 || r.hard.Commit == 0 {
		return false
	}

	term, exists := r.TermAt(r.hard.Commit)

	return exists && term == r.hard.Term
}

// readCommitBarrier returns the committed index that
// may be captured before starting a fresh read quorum.
//
// This method does not modify Raft state.
func (r *Raft) readCommitBarrier() (uint64, error) {
	if r.volatile.Role != Leader {
		return 0, ErrNotLeader
	}

	if !r.hasCommittedCurrentTerm() {
		return 0, ErrReadNotReady
	}

	return r.hard.Commit, nil
}
