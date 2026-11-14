package wal

import (
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// ValidateCompaction checks whether a snapshot boundary
// is valid for the current persisted Raft state.
//
// It does not modify the WAL or delete any entries.
func ValidateCompaction(
	state raft.PersistentState,
	index uint64,
	term uint64,
) error {
	if index == 0 {
		return fmt.Errorf(
			"%w: compaction index must be positive",
			ErrInvalid,
		)
	}

	if term == 0 {
		return fmt.Errorf(
			"%w: compaction term must be positive",
			ErrInvalid,
		)
	}

	if index > state.HardState.Commit {
		return fmt.Errorf(
			"%w: cannot compact uncommitted entries",
			ErrInvalid,
		)
	}

	base := state.Snapshot.LastIncludedIndex
	baseTerm := state.Snapshot.LastIncludedTerm

	if index < base {
		return fmt.Errorf(
			"%w: compaction index regresses",
			ErrInvalid,
		)
	}

	if index == base {
		if term != baseTerm {
			return fmt.Errorf(
				"%w: snapshot boundary term mismatch",
				ErrInvalid,
			)
		}
		return nil
	}

	offset := index - base - 1

	if offset >= uint64(len(state.Log)) {
		return fmt.Errorf(
			"%w: compaction index exceeds log",
			ErrInvalid,
		)
	}

	if state.Log[offset].Index != index ||
		state.Log[offset].Term != term {
		return fmt.Errorf(
			"%w: snapshot boundary does not match log",
			ErrInvalid,
		)
	}

	return nil
}
