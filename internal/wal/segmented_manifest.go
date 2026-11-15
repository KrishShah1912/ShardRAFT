package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const segmentedManifestVersion = 1

type segmentedManifest struct {
	Version           int    `json:"version"`
	FirstSegment      uint64 `json:"first_segment"`
	LastSegment       uint64 `json:"last_segment,omitempty"`
	CheckpointSegment uint64 `json:"checkpoint_segment,omitempty"`
}

func (m segmentedManifest) validate() error {
	if m.Version != segmentedManifestVersion {
		return fmt.Errorf(
			"%w: unsupported manifest version",
			ErrInvalid,
		)
	}

	if m.FirstSegment == 0 {
		return fmt.Errorf(
			"%w: invalid first segment",
			ErrInvalid,
		)
	}

	if m.LastSegment != 0 &&
		m.LastSegment < m.FirstSegment {
		return fmt.Errorf(
			"%w: last segment precedes first segment",
			ErrInvalid,
		)
	}

	if m.CheckpointSegment != 0 &&
		m.CheckpointSegment != m.FirstSegment {
		return fmt.Errorf(
			"%w: checkpoint segment must equal first segment",
			ErrInvalid,
		)
	}

	return nil
}

func manifestPath(walPath string) string {
	return filepath.Join(segmentDirectory(walPath), "MANIFEST")
}

func writeSegmentedManifest(
	walPath string,
	manifest segmentedManifest,
) error {
	return writeSegmentedManifestWithOps(
		walPath,
		manifest,
		defaultPhysicalCompactionOps(),
	)
}

func writeSegmentedManifestWithOps(
	walPath string,
	manifest segmentedManifest,
	ops physicalCompactionOps,
) error {
	if err := manifest.validate(); err != nil {
		return err
	}

	if ops.rename == nil || ops.syncDir == nil {
		return fmt.Errorf(
			"%w: missing manifest filesystem operations",
			ErrInvalid,
		)
	}

	dir := segmentDirectory(walPath)

	temp, err := os.CreateTemp(dir, ".manifest-*")
	if err != nil {
		return err
	}

	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()

	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}

	if err := writeFull(temp, data); err != nil {
		return err
	}

	if err := temp.Sync(); err != nil {
		return err
	}

	if err := temp.Close(); err != nil {
		return err
	}

	if err := ops.rename(
		tempPath,
		manifestPath(walPath),
	); err != nil {
		return fmt.Errorf(
			"publish manifest rename: %w",
			err,
		)
	}

	if err := ops.syncDir(dir); err != nil {
		return fmt.Errorf(
			"sync manifest directory: %w",
			err,
		)
	}

	return nil
}

func readSegmentedManifest(
	walPath string,
) (segmentedManifest, bool, error) {
	var manifest segmentedManifest

	data, err := os.ReadFile(manifestPath(walPath))
	if os.IsNotExist(err) {
		return manifest, false, nil
	}
	if err != nil {
		return manifest, false, err
	}

	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, false, fmt.Errorf(
			"%w: invalid segmented manifest",
			ErrCorrupt,
		)
	}

	if err := manifest.validate(); err != nil {
		return manifest, false, fmt.Errorf(
			"%w: invalid segmented manifest fields: %v",
			ErrCorrupt,
			err,
		)
	}

	return manifest, true, nil
}
