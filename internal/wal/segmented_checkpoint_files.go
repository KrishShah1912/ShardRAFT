package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const checkpointExtension = ".chk"

func checkpointFilename(number uint64) string {
	return fmt.Sprintf(
		"%020d%s",
		number,
		checkpointExtension,
	)
}

func checkpointPath(
	walPath string,
	number uint64,
) string {
	return filepath.Join(
		segmentDirectory(walPath),
		checkpointFilename(number),
	)
}

// listCheckpointFiles discovers durable checkpoint files.
//
// Discovery does not imply publication. Only MANIFEST
// may authorize a checkpoint for recovery.
func listCheckpointFiles(
	walPath string,
) ([]uint64, error) {
	entries, err := os.ReadDir(segmentDirectory(walPath))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var numbers []uint64

	for _, entry := range entries {
		name := entry.Name()

		if !strings.HasSuffix(
			name,
			checkpointExtension,
		) {
			continue
		}

		if entry.IsDir() {
			return nil, fmt.Errorf(
				"%w: checkpoint is a directory: %s",
				ErrCorrupt,
				name,
			)
		}

		numberPart := strings.TrimSuffix(
			name,
			checkpointExtension,
		)

		if len(numberPart) != 20 {
			return nil, fmt.Errorf(
				"%w: invalid checkpoint filename: %s",
				ErrCorrupt,
				name,
			)
		}

		number, err := strconv.ParseUint(
			numberPart,
			10,
			64,
		)
		if err != nil || number == 0 {
			return nil, fmt.Errorf(
				"%w: invalid checkpoint number: %s",
				ErrCorrupt,
				name,
			)
		}

		numbers = append(numbers, number)
	}

	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})

	return numbers, nil
}
