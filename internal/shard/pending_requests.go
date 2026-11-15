package shard

import (
	"errors"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

var ErrRequestPending = errors.New(
	"request already proposed but not yet committed",
)

// checkPendingRequest scans uncommitted Raft entries.
//
// The caller must hold s.mu.
func (s *Shard) checkPendingRequest(
	command kvstore.Command,
) (bool, error) {
	if command.ClientID == "" {
		return false, nil
	}

	for _, entry := range s.node.PendingEntries() {
		if entry.Type != raft.EntryNormal {
			continue
		}

		existing, err := kvstore.DecodeCommand(entry.Data)
		if err != nil {
			return false, err
		}

		if existing.ClientID != command.ClientID ||
			existing.RequestID != command.RequestID {
			continue
		}

		if existing != command {
			return true, kvstore.ErrRequestConflict
		}

		return true, ErrRequestPending
	}

	return false, nil
}
