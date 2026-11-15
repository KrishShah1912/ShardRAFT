package raft

import "fmt"

// CheckInvariants validates the internal safety
// properties of a Raft node.
//
// It does not modify the node's state.
func (r *Raft) CheckInvariants() error {
	if r.volatile.AppliedIndex > r.hard.Commit {
		return fmt.Errorf(
			"raft node %d: applied index %d exceeds commit index %d",
			r.id,
			r.volatile.AppliedIndex,
			r.hard.Commit,
		)
	}

	if r.hard.Commit > r.lastIndex() {
		return fmt.Errorf(
			"raft node %d: commit index %d exceeds last log index %d",
			r.id,
			r.hard.Commit,
			r.lastIndex(),
		)
	}

	if r.snapshotIndex > r.hard.Commit {
		return fmt.Errorf(
			"raft node %d: snapshot index %d exceeds commit index %d",
			r.id,
			r.snapshotIndex,
			r.hard.Commit,
		)
	}

	if r.volatile.CommitIndex != r.hard.Commit {
		return fmt.Errorf(
			"raft node %d: volatile commit %d differs from persistent commit %d",
			r.id,
			r.volatile.CommitIndex,
			r.hard.Commit,
		)
	}

	expected := r.snapshotIndex + 1

	for _, entry := range r.log {
		if entry.Index != expected {
			return fmt.Errorf(
				"raft node %d: invalid log index %d, expected %d",
				r.id,
				entry.Index,
				expected,
			)
		}

		if entry.Term == 0 {
			return fmt.Errorf(
				"raft node %d: log entry %d has zero term",
				r.id,
				entry.Index,
			)
		}

		if entry.Term > r.hard.Term {
			return fmt.Errorf(
				"raft node %d: log entry %d term %d exceeds current term %d",
				r.id,
				entry.Index,
				entry.Term,
				r.hard.Term,
			)
		}

		expected++
	}

	return nil
}
