package raftdriver

import (
	"errors"
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

// CompactKV creates a durable KV snapshot and
// compacts the corresponding Raft log prefix.
//
// The caller must serialize this operation with
// Tick, Step, Propose, Process, and all other
// mutations of the Raft node.
//
// A failure after snapshot persistence must be
// treated as a stop-and-recover condition.
func (d *Driver) CompactKV(
	node *raft.Raft,
	storage *wal.WAL,
	store *kvstore.Store,
	snapshotPath string,
) error {
	if storage == nil {
		return errors.New("nil WAL storage")
	}

	return d.compactKVWith(
		node,
		storage,
		store,
		snapshotPath,
		storage.CompactPrefix,
	)
}

// compactKVWith is the internal compaction workflow.
//
// The compact function is injectable so tests can
// simulate failures at the WAL compaction stage.
func (d *Driver) compactKVWith(
	node *raft.Raft,
	storage *wal.WAL,
	store *kvstore.Store,
	snapshotPath string,
	compact func(index, term uint64) error,
) error {
	if d.stopped {
		return ErrStopped
	}

	if node == nil || storage == nil || store == nil {
		return errors.New("nil compaction dependency")
	}

	if compact == nil {
		return errors.New("nil WAL compaction callback")
	}

	if snapshotPath == "" {
		return errors.New("empty snapshot path")
	}

	if d.node != node || d.storage != storage {
		return errors.New(
			"compaction dependencies do not match driver",
		)
	}

	// Drain pending Raft work.
	if err := d.Process(); err != nil {
		return err
	}

	index := store.AppliedIndex()

	if index == 0 {
		return errors.New("no applied entries to compact")
	}

	if index > node.Status().Commit {
		return errors.New(
			"KV applied index exceeds Raft commit index",
		)
	}

	term, ok := node.TermAt(index)
	if !ok || term == 0 {
		return errors.New(
			"cannot determine snapshot boundary term",
		)
	}

	base := node.SnapshotMetadata()

	if index < base.LastIncludedIndex {
		return errors.New(
			"KV applied index precedes snapshot boundary",
		)
	}

	if index == base.LastIncludedIndex {
		return nil
	}

	data := store.Snapshot()

	if store.AppliedIndex() != index {
		return errors.New(
			"KV state changed during snapshot capture",
		)
	}

	s := snapshot.New(index, term, data)

	// Phase 1: Save the KV snapshot.
	if err := snapshot.Save(snapshotPath, s); err != nil {
		return d.stop(fmt.Errorf(
			"save KV snapshot: %w", err,
		))
	}

	// Phase 2: Verify the saved snapshot.
	verified, err := snapshot.Load(snapshotPath)
	if err != nil {
		return d.stop(fmt.Errorf(
			"verify KV snapshot: %w", err,
		))
	}

	if verified.AppliedIndex != index ||
		verified.AppliedTerm != term {
		return d.stop(errors.New(
			"persisted snapshot boundary mismatch",
		))
	}

	// Phase 3: Compact the WAL.
	// Tests can inject a failure here.
	if err := compact(index, term); err != nil {
		return d.stop(fmt.Errorf(
			"compact WAL prefix: %w", err,
		))
	}

	// Phase 4: Compact the in-memory Raft log.
	if err := node.CompactPrefix(index, term); err != nil {
		return d.stop(fmt.Errorf(
			"compact in-memory Raft log: %w", err,
		))
	}

	return nil
}
