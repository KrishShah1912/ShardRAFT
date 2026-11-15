package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const segmentExtension = ".seg"

func segmentDirectory(walPath string) string {
	return walPath + ".segments"
}

func segmentFilename(number uint64) string {
	return fmt.Sprintf("%020d%s", number, segmentExtension)
}

func segmentPath(walPath string, number uint64) string {
	return filepath.Join(
		segmentDirectory(walPath),
		segmentFilename(number),
	)
}

// listSegments discovers segment files in ascending order.
//
// A published manifest permits older segment prefixes
// to be retired. All active segments must be consecutive.
func listSegments(walPath string) ([]uint64, error) {
	dir := segmentDirectory(walPath)

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	manifest, found, err := readSegmentedManifest(walPath)
	if err != nil {
		return nil, err
	}

	var numbers []uint64

	for _, entry := range entries {
		name := entry.Name()

		if entry.IsDir() {
			return nil, fmt.Errorf(
				"unexpected directory in WAL segments: %s",
				name,
			)
		}

		// Ignore known metadata and temporary files.
		if name == "MANIFEST" ||
			strings.HasPrefix(name, ".manifest-") ||
			strings.HasPrefix(name, ".checkpoint-") ||
			strings.HasSuffix(name, checkpointExtension) {
			continue
		}

		if !strings.HasSuffix(name, segmentExtension) {
			return nil, fmt.Errorf(
				"unexpected WAL segment file: %s",
				name,
			)
		}

		numberPart := strings.TrimSuffix(
			name, segmentExtension,
		)

		if len(numberPart) != 20 {
			return nil, fmt.Errorf(
				"invalid WAL segment name: %s",
				name,
			)
		}

		number, err := strconv.ParseUint(
			numberPart, 10, 64,
		)
		if err != nil || number == 0 {
			return nil, fmt.Errorf(
				"invalid WAL segment number: %s",
				name,
			)
		}

		if found && number < manifest.FirstSegment {
			continue
		}

		numbers = append(numbers, number)
	}

	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})

	if found {
		if len(numbers) == 0 ||
			numbers[0] != manifest.FirstSegment {
			return nil, fmt.Errorf(
				"%w: manifest checkpoint segment %d missing",
				ErrCorrupt,
				manifest.FirstSegment,
			)
		}
	} else if len(numbers) > 0 && numbers[0] != 1 {
		return nil, fmt.Errorf(
			"%w: initial WAL segment missing",
			ErrCorrupt,
		)
	}

	for i := 1; i < len(numbers); i++ {
		if numbers[i] != numbers[i-1]+1 {
			return nil, fmt.Errorf(
				"%w: WAL segment numbering gap: %d to %d",
				ErrCorrupt,
				numbers[i-1],
				numbers[i],
			)
		}
	}

	return numbers, nil
}
