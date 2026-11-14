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
// The compact callback is injectable so tests can
// simulate failures during logical WAL compaction.
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

	// Phase 1: Persist and apply all pending Raft work.
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

	// Nothing new has been applied since the
	// previous snapshot.
	if index == base.LastIncludedIndex {
		return nil
	}

	// Determine the WAL path before changing any
	// durable snapshot or compaction state.
	walPath, err := storage.Path()
	if err != nil {
		return d.stop(fmt.Errorf(
			"determine WAL path: %w",
			err,
		))
	}

	// Phase 2: Capture the KV state.
	data := store.Snapshot()

	if store.AppliedIndex() != index {
		return errors.New(
			"KV state changed during snapshot capture",
		)
	}

	s := snapshot.New(index, term, data)

	// Save the snapshot atomically.
	if err := snapshot.Save(snapshotPath, s); err != nil {
		return d.stop(fmt.Errorf(
			"save KV snapshot: %w",
			err,
		))
	}

	// Read the snapshot back to verify its
	// persisted metadata and contents.
	verified, err := snapshot.Load(snapshotPath)
	if err != nil {
		return d.stop(fmt.Errorf(
			"verify KV snapshot: %w",
			err,
		))
	}

	if verified.AppliedIndex != index ||
		verified.AppliedTerm != term {
		return d.stop(errors.New(
			"persisted snapshot boundary mismatch",
		))
	}

	// Phase 3: Logically compact the WAL.
	//
	// This durably records the new snapshot boundary
	// while retaining the historical WAL records.
	if err := compact(index, term); err != nil {
		return d.stop(fmt.Errorf(
			"compact WAL prefix: %w",
			err,
		))
	}

	// Phase 4: Compact the in-memory Raft log.
	if err := node.CompactPrefix(index, term); err != nil {
		return d.stop(fmt.Errorf(
			"compact in-memory Raft log: %w",
			err,
		))
	}

	// Phase 5: Physically compact the WAL.
	//
	// Rewrite the WAL into a checkpoint containing:
	//   - Current hard state
	//   - Snapshot boundary metadata
	//   - Remaining Raft log entries
	//
	// The rewrite uses a temporary file, fsync,
	// atomic rename, and directory synchronization.
	//
	// Failure requires stopping the driver and
	// recovering from the durable on-disk state.
	if err := storage.RewriteCheckpoint(walPath); err != nil {
		return d.stop(fmt.Errorf(
			"physically compact WAL: %w",
			err,
		))
	}

	return nil
}
