package wal

import (
	"fmt"
	"os"
	"reflect"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// PublishCheckpoint publishes a standalone checkpoint and
// begins a new WAL segment generation.
//
// The caller must serialize all WAL mutations and ensure
// the matching durable KV snapshot already exists.
func (w *SegmentedWAL) PublishCheckpoint() error {
	if w.closed || w.active == nil || w.unusable {
		return ErrUnusable
	}
	if !w.recovered {
		return ErrNotRecovered
	}
	if w.state.Snapshot.LastIncludedIndex == 0 {
		return fmt.Errorf(
			"%w: no compacted snapshot boundary",
			ErrInvalid,
		)
	}

	if err := w.active.Sync(); err != nil {
		w.unusable = true
		return err
	}

	temp, err := w.prepareCheckpoint()
	if err != nil {
		return err
	}

	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()

	newNumber := w.number + 1
	checkpointFile := checkpointPath(w.path, newNumber)
	newSegmentPath := segmentPath(w.path, newNumber)

	if err := temp.Close(); err != nil {
		return err
	}

	// Never overwrite a previously staged checkpoint.
	if _, err := os.Lstat(checkpointFile); err == nil {
		return fmt.Errorf(
			"%w: checkpoint file already exists",
			ErrInvalid,
		)
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(tempPath, checkpointFile); err != nil {
		return err
	}

	ops := defaultPhysicalCompactionOps()

	if err := ops.syncDir(segmentDirectory(w.path)); err != nil {
		w.unusable = true
		return err
	}

	// Create the new replay segment before publication.
	// If publication is interrupted, the old generation
	// can still replay this empty segment.
	newActive, err := os.OpenFile(
		newSegmentPath,
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0644,
	)
	if err != nil {
		w.unusable = true
		return err
	}

	if err := newActive.Sync(); err != nil {
		_ = newActive.Close()
		w.unusable = true
		return err
	}

	if err := ops.syncDir(segmentDirectory(w.path)); err != nil {
		_ = newActive.Close()
		w.unusable = true
		return err
	}

	// Switch writers before publishing the new manifest.
	if err := w.active.Close(); err != nil {
		_ = newActive.Close()
		w.unusable = true
		return err
	}

	w.active = newActive
	w.number = newNumber
	w.size = 0

	manifest := segmentedManifest{
		Version:           segmentedManifestVersion,
		FirstSegment:      newNumber,
		CheckpointSegment: newNumber,
	}

	manifestOps := defaultPhysicalCompactionOps()
	if w.manifestPublishOps != nil {
		manifestOps = *w.manifestPublishOps
	}

	if err := writeSegmentedManifestWithOps(
		w.path,
		manifest,
		manifestOps,
	); err != nil {
		w.unusable = true
		return err
	}

	return nil
}

// VerifyPublishedCheckpoint reconstructs the published
// generation and compares its persistent Raft state.
func (w *SegmentedWAL) VerifyPublishedCheckpoint() error {
	if w.closed || w.unusable || !w.recovered {
		return ErrUnusable
	}

	reopened, err := OpenSegmented(w.path, w.options)
	if err != nil {
		return err
	}
	defer reopened.Close()

	state, err := reopened.Recover()
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(
		copyState(state),
		copyState(w.state),
	) {
		return fmt.Errorf(
			"%w: published checkpoint state mismatch",
			ErrCorrupt,
		)
	}

	return nil
}

var _ = raft.PersistentState{}
