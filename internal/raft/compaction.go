package raft

import (
	"errors"
	"fmt"
)

// CompactPrefix removes an applied, durable log prefix
// from the in-memory Raft log.
//
// Preconditions:
// 1. A matching KV snapshot has been saved and verified.
// 2. The WAL has durably recorded the same boundary.
// 3. No Ready batch is awaiting persistence/application.
// 4. The caller serializes all Raft operations.
//
// This method does not persist anything itself.
func (r *Raft) CompactPrefix(index, term uint64) error {
	if index == 0 || term == 0 {
		return errors.New("invalid compaction boundary")
	}

	if index < r.snapshotIndex {
		return errors.New("compaction boundary regresses")
	}

	if index == r.snapshotIndex {
		if term != r.snapshotTerm {
			return errors.New("snapshot boundary term mismatch")
		}
		return nil
	}

	if index > r.hard.Commit ||
		index > r.volatile.AppliedIndex {
		return errors.New("cannot compact unapplied entries")
	}

	actualTerm, ok := r.TermAt(index)
	if !ok || actualTerm != term {
		return errors.New("compaction boundary term mismatch")
	}

	// Require all persistence work to have been
	// acknowledged before changing the log base.
	rd := r.Ready()

	if rd.HardState != nil ||
		rd.TruncateFrom != 0 ||
		len(rd.Entries) != 0 ||
		len(rd.CommittedEntries) != 0 ||
		len(rd.Messages) != 0 {
		return errors.New(
			"cannot compact while Ready work is pending",
		)
	}

	oldBase := r.snapshotIndex
	removeCount := index - oldBase

	if removeCount > uint64(len(r.log)) ||
		removeCount > uint64(len(r.stableLog)) {
		return fmt.Errorf(
			"compaction boundary exceeds available log",
		)
	}

	r.log = cloneEntries(r.log[int(removeCount):])
	r.stableLog = cloneEntries(
		r.stableLog[int(removeCount):],
	)

	r.snapshotIndex = index
	r.snapshotTerm = term

	return nil
}
