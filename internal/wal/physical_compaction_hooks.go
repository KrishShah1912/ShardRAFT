package wal

import (
	"os"
)

// physicalCompactionOps contains filesystem operations
// used during physical WAL compaction.
//
// Production uses the actual operating system calls.
// Tests can replace individual operations to simulate
// failures at specific stages.
type physicalCompactionOps struct {
	rename  func(oldPath, newPath string) error
	syncDir func(path string) error
}

// defaultPhysicalCompactionOps returns the real
// filesystem operations.
func defaultPhysicalCompactionOps() physicalCompactionOps {
	return physicalCompactionOps{
		rename: os.Rename,
		syncDir: func(path string) error {
			dir, err := os.Open(path)
			if err != nil {
				return err
			}

			syncErr := dir.Sync()
			closeErr := dir.Close()

			if syncErr != nil {
				return syncErr
			}

			return closeErr
		},
	}
}
