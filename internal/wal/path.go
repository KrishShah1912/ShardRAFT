package wal

import (
	"fmt"
	"os"
)

// Path returns the permanent filesystem path
// associated with this WAL.
//
// The path remains unchanged after physical
// checkpoint rewriting and atomic replacement.
func (w *WAL) Path() (string, error) {
	if err := w.checkWritable(); err != nil {
		return "", err
	}

	if w.path == "" {
		return "", fmt.Errorf(
			"%w: WAL path is unavailable",
			ErrInvalid,
		)
	}

	// Physical rewriting requires a real file.
	// Keep rejecting injected file implementations.
	if _, ok := w.file.(*os.File); !ok {
		return "", fmt.Errorf(
			"%w: WAL does not use a regular file",
			ErrInvalid,
		)
	}

	return w.path, nil
}
