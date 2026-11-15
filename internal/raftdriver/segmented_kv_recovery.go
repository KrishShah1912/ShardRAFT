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

// RecoverSegmentedKVDriver restores a Raft node and KV state
// from segmented WAL and a durable KV snapshot.
//
// The caller owns the returned WAL and must close it.
// Call driver.Process() before accepting new Raft activity.
func RecoverSegmentedKVDriver(
	walPath string,
	snapshotPath string,
	options wal.SegmentOptions,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
	send SendFunc,
) (
	*raft.Raft,
	*wal.SegmentedWAL,
	*Driver,
	*kvstore.Store,
	error,
) {
	if snapshotPath == "" {
		return nil, nil, nil, nil, errors.New(
			"snapshot path is required",
		)
	}

	if send == nil {
		return nil, nil, nil, nil, errors.New(
			"send callback is required",
		)
	}

	node, storage, err := RecoverSegmentedNode(
		walPath,
		options,
		id,
		peers,
		electionTimeout,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	fail := func(err error) (
		*raft.Raft,
		*wal.SegmentedWAL,
		*Driver,
		*kvstore.Store,
		error,
	) {
		_ = storage.Close()
		return nil, nil, nil, nil, err
	}

	store := kvstore.New()
	base := node.SnapshotMetadata()

	s, loadErr := snapshot.Load(snapshotPath)

	if loadErr != nil {
		if !errors.Is(loadErr, os.ErrNotExist) {
			return fail(fmt.Errorf(
				"load KV snapshot: %w", loadErr,
			))
		}

		if base.LastIncludedIndex != 0 {
			return fail(errors.New(
				"compacted segmented WAL requires a KV snapshot",
			))
		}
	} else {
		if s.AppliedIndex > node.Status().Commit {
			return fail(errors.New(
				"KV snapshot exceeds Raft commit index",
			))
		}

		if s.AppliedIndex < base.LastIncludedIndex {
			return fail(errors.New(
				"KV snapshot is older than segmented WAL boundary",
			))
		}

		if s.AppliedIndex == base.LastIncludedIndex &&
			s.AppliedTerm != base.LastIncludedTerm {
			return fail(errors.New(
				"KV snapshot term does not match WAL boundary",
			))
		}

		if s.AppliedIndex > 0 {
			term, ok := node.TermAt(s.AppliedIndex)
			if !ok || term != s.AppliedTerm {
				return fail(errors.New(
					"KV snapshot index/term does not match Raft history",
				))
			}
		}

		if err := store.Restore(
			s.AppliedIndex,
			s.Data,
		); err != nil {
			return fail(fmt.Errorf(
				"restore KV snapshot: %w", err,
			))
		}

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
