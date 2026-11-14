package raftdriver

import (
	"errors"
	"fmt"
	"os"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

// RecoverKVDriver restores Raft from the WAL and,
// when present, restores the KV store from a snapshot.
//
// The caller must call driver.Process() successfully
// before allowing new Raft activity.
//
// The caller owns the returned WAL and must close it.
func RecoverKVDriver(
	walPath string,
	snapshotPath string,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
	send SendFunc,
) (*raft.Raft, *wal.WAL, *Driver, *kvstore.Store, error) {
	if send == nil {
		return nil, nil, nil, nil, errors.New(
			"send callback is required",
		)
	}

	node, storage, err := RecoverNode(
		walPath, id, peers, electionTimeout,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	fail := func(err error) (
		*raft.Raft, *wal.WAL, *Driver, *kvstore.Store, error,
	) {
		storage.Close()
		return nil, nil, nil, nil, err
	}

	store := kvstore.New()

	s, err := snapshot.Load(snapshotPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fail(fmt.Errorf("load snapshot: %w", err))
	}

	if err == nil {
		// Ready contains all committed entries on recovery
		// because the restored Raft applied index is zero.
		committed := node.Ready().CommittedEntries

		if s.AppliedIndex > node.Status().Commit {
			return fail(errors.New(
				"snapshot index exceeds Raft commit index",
			))
		}

		if s.AppliedIndex > 0 {
			// Current implementation retains the full log,
			// so the snapshot boundary must be present.
			if s.AppliedIndex > uint64(len(committed)) {
				return fail(errors.New(
					"snapshot boundary missing from WAL",
				))
			}

			boundary := committed[s.AppliedIndex-1]

			if boundary.Index != s.AppliedIndex ||
				boundary.Term != s.AppliedTerm {
				return fail(errors.New(
					"snapshot index/term does not match WAL",
				))
			}
		}

		if err := store.Restore(s.AppliedIndex, s.Data); err != nil {
			return fail(fmt.Errorf("restore KV state: %w", err))
		}
	}

	driver, err := New(node, storage, store.Apply, send)
	if err != nil {
		return fail(err)
	}

	return node, storage, driver, store, nil
}
