package shard

import (
	"errors"
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

var ErrStopped = errors.New("shard stopped; recovery required")

// Compact saves a durable KV snapshot, compacts the
// Raft log, publishes a WAL checkpoint, and retires
// obsolete WAL segments.
//
// All shard mutations must be serialized with Compact.
func (s *Shard) Compact(snapshotPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.reconcilePendingReads()

	if s.closed {
		return ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return ErrStopped
	}

	if snapshotPath != s.snapshotPath {
		return errors.New(
			"compaction snapshot path must match recovery path",
		)
	}

	// Persist and apply all pending Raft work.
	if err := s.processDriver(); err != nil {
		return fmt.Errorf("process pending Raft work: %w", err)
	}

	// Capture data, dedup history, and index together.
	index, data, dedup := s.store.CaptureSnapshot()

	if index == 0 {
		return errors.New("no applied entries to compact")
	}

	status := s.node.Status()
	if index > status.Commit {
		return errors.New("applied index exceeds commit index")
	}

	term, ok := s.node.TermAt(index)
	if !ok || term == 0 {
		return errors.New("cannot determine snapshot term")
	}

	base := s.node.SnapshotMetadata()
	if index < base.LastIncludedIndex {
		return errors.New("applied index precedes snapshot boundary")
	}

	if index == base.LastIncludedIndex {
		return nil
	}

	// Snapshot must be durable before log compaction.
	snap := snapshot.NewWithDedup(index, term, data, dedup)

	if err := snapshot.Save(snapshotPath, snap); err != nil {
		return fmt.Errorf("save KV snapshot: %w", err)
	}

	verified, err := snapshot.Load(snapshotPath)
	if err != nil {
		return s.stop(fmt.Errorf(
			"verify KV snapshot: %w", err,
		))
	}

	if verified.AppliedIndex != index ||
		verified.AppliedTerm != term {
		return s.stop(errors.New(
			"persisted snapshot boundary mismatch",
		))
	}

	// Record the compacted boundary durably in WAL.
	if err := s.storage.CompactPrefix(index, term); err != nil {
		return s.stop(fmt.Errorf(
			"compact segmented WAL: %w", err,
		))
	}

	// Compact the corresponding in-memory Raft log.
	if err := s.node.CompactPrefix(index, term); err != nil {
		return s.stop(fmt.Errorf(
			"compact in-memory Raft log: %w", err,
		))
	}

	// Publish a new independently recoverable WAL generation.
	if err := s.storage.PublishCheckpoint(); err != nil {
		return s.stop(fmt.Errorf(
			"publish WAL checkpoint: %w", err,
		))
	}

	if err := s.storage.VerifyPublishedCheckpoint(); err != nil {
		return s.stop(fmt.Errorf(
			"verify published checkpoint: %w", err,
		))
	}

	// Only delete old segments after successful verification.
	if err := s.storage.RetireObsoleteSegments(); err != nil {
		return s.stop(fmt.Errorf(
			"retire obsolete segments: %w", err,
		))
	}

	return nil
}

// stop disables subsequent shard operations.
// The shard must be closed and reopened to recover.
func (s *Shard) stop(err error) error {
	s.stopped = true
	s.failPendingReads(ErrStopped)
	return fmt.Errorf("%w: %v", ErrStopped, err)
}
