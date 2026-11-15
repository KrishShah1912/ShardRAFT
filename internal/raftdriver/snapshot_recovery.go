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

// RecoverKVDriver restores Raft and KV state.
//
// A compacted WAL requires a snapshot at or ahead
// of its compacted boundary. A newer snapshot is
// accepted only when its index and term match the
// recovered Raft history.
//
// Call driver.Process() before new Raft activity.
func RecoverKVDriver(
	walPath string,
	snapshotPath string,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
	send SendFunc,
) (*raft.Raft, *wal.WAL, *Driver, *kvstore.Store, error) {
	return recoverKVDriverWithEngine(
		walPath, snapshotPath, id, peers,
		electionTimeout, send, nil,
	)
}

// recoverKVDriverWithEngine supports an optional KV engine.
// The caller owns the engine and is responsible for closing it.
func recoverKVDriverWithEngine(
	walPath string,
	snapshotPath string,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
	send SendFunc,
	engine kvstore.Engine,
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
		_ = storage.Close()
		return nil, nil, nil, nil, err
	}

	store := kvstore.New()
	if engine != nil {
		store, err = kvstore.NewWithEngine(engine)
		if err != nil {
			return fail(fmt.Errorf(
				"attach KV engine: %w", err,
			))
		}
	}
	base := node.SnapshotMetadata()

	s, loadErr := snapshot.Load(snapshotPath)

	if loadErr != nil {
		if !errors.Is(loadErr, os.ErrNotExist) {
			return fail(fmt.Errorf(
				"load snapshot: %w", loadErr,
			))
		}

		if base.LastIncludedIndex != 0 {
			return fail(errors.New(
				"compacted WAL requires a matching snapshot",
			))
		}
	} else {
		if s.AppliedIndex > node.Status().Commit {
			return fail(errors.New(
				"snapshot index exceeds Raft commit index",
			))
		}

		if base.LastIncludedIndex != 0 {
			if s.AppliedIndex < base.LastIncludedIndex {
				return fail(errors.New(
					"snapshot is older than compacted WAL boundary",
				))
			}

			if s.AppliedIndex == base.LastIncludedIndex &&
				s.AppliedTerm != base.LastIncludedTerm {
				return fail(errors.New(
					"snapshot does not match compacted WAL boundary",
				))
			}
		}

		if s.AppliedIndex > 0 {
			term, ok := node.TermAt(s.AppliedIndex)

			if !ok || term != s.AppliedTerm {
				return fail(errors.New(
					"snapshot index/term does not match WAL",
				))
			}
		}

		if err := store.Restore(
			s.AppliedIndex, s.Data,
		); err != nil {
			return fail(fmt.Errorf(
				"restore KV state: %w", err,
			))
		}

		// Restore request history from the snapshot.
		if err := store.RestoreDedup(s.Dedup); err != nil {
			return fail(fmt.Errorf(
				"restore request history: %w", err,
			))
		}
	}

	driver, err := New(
		node,
		storage,
		store.Apply,
		send,
	)
	if err != nil {
		return fail(err)
	}

	return node, storage, driver, store, nil
}
