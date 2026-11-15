package wal

import (
	"fmt"
	"os"
)

// validateCheckpointReference checks whether an explicit
// checkpoint reference resolves to a regular file.
//
// It does not validate the checkpoint's contents.
// Full record and checksum validation happens during recovery.
func validateCheckpointReference(
	walPath string,
	manifest segmentedManifest,
) error {
	if err := manifest.validate(); err != nil {
		return err
	}

	// Legacy manifests use a checkpoint embedded in
	// the first active .seg file.
	if manifest.CheckpointSegment == 0 {
		return nil
	}

	path := checkpointPath(
		walPath,
		manifest.CheckpointSegment,
	)

	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf(
			"%w: checkpoint file unavailable: %v",
			ErrCorrupt,
			err,
		)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf(
			"%w: checkpoint is not a regular file",
			ErrCorrupt,
		)
	}

	if info.Size() == 0 {
		return fmt.Errorf(
			"%w: checkpoint file is empty",
			ErrCorrupt,
		)
	}

	return nil
}
