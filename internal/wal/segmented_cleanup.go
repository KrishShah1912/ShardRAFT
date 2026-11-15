package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RetireObsoleteSegments removes WAL segments older than
// the checkpoint generation published in MANIFEST.
//
// The caller must serialize this operation with all
// other WAL mutations.
func (w *SegmentedWAL) RetireObsoleteSegments() error {
	if w.closed || w.active == nil || w.unusable {
		return ErrUnusable
	}

	if !w.recovered {
		return ErrNotRecovered
	}

	manifest, found, err := readSegmentedManifest(w.path)
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf(
			"%w: no published checkpoint manifest",
			ErrInvalid,
		)
	}

	// Verify the published generation before deleting
	// anything from previous generations.
	if err := w.VerifyPublishedCheckpoint(); err != nil {
		return fmt.Errorf(
			"verify checkpoint before cleanup: %w",
			err,
		)
	}

	dir := segmentDirectory(w.path)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	removed := false

	for _, entry := range entries {
		if entry.IsDir() {
			return fmt.Errorf(
				"%w: unexpected directory in WAL",
				ErrCorrupt,
			)
		}

		name := entry.Name()

		if !strings.HasSuffix(name, segmentExtension) {
			continue
		}

		numberPart := strings.TrimSuffix(
			name,
			segmentExtension,
		)

		if len(numberPart) != 20 {
			return fmt.Errorf(
				"%w: invalid segment name",
				ErrCorrupt,
			)
		}

		number, err := strconv.ParseUint(
			numberPart, 10, 64,
		)
		if err != nil || number == 0 {
			return fmt.Errorf(
				"%w: invalid segment number",
				ErrCorrupt,
			)
		}

		if number >= manifest.FirstSegment {
			continue
		}

		path := filepath.Join(dir, name)

		if err := os.Remove(path); err != nil {
			return fmt.Errorf(
				"remove obsolete segment %d: %w",
				number,
				err,
			)
		}

		removed = true
	}

	if removed {
		ops := defaultPhysicalCompactionOps()

		if err := ops.syncDir(dir); err != nil {
			w.unusable = true
			return fmt.Errorf(
				"sync WAL directory after cleanup: %w",
				err,
			)
		}
	}

	return nil
}
