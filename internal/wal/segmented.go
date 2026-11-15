package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

const DefaultSegmentSize int64 = 16 << 20

type SegmentOptions struct {
	MaxSegmentSize int64
}

func (o SegmentOptions) validate() error {
	if o.MaxSegmentSize <= headerSize {
		return fmt.Errorf(
			"segment size must exceed WAL header size",
		)
	}
	return nil
}

// SegmentedWAL owns a sequence of checksummed WAL segments.
//
// The existing single-file WAL remains unchanged.
type SegmentedWAL struct {
	// Test hook for injecting a failure after a record header.
	afterHeaderWrite   func() error
	manifestPublishOps *physicalCompactionOps

	state     raft.PersistentState
	path      string
	options   SegmentOptions
	active    *os.File
	number    uint64
	size      int64
	closed    bool
	recovered bool
	unusable  bool
}

// OpenSegmented creates a new WAL or opens an existing WAL.
//
// Existing segments are never silently recreated.
func OpenSegmented(
	path string,
	options SegmentOptions,
) (*SegmentedWAL, error) {
	if path == "" {
		return nil, errors.New("empty WAL path")
	}

	if options.MaxSegmentSize == 0 {
		options.MaxSegmentSize = DefaultSegmentSize
	}

	if err := options.validate(); err != nil {
		return nil, err
	}

	path = filepath.Clean(path)
	dir := segmentDirectory(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	manifest, hasManifest, err := readSegmentedManifest(path)
	if err != nil {
		return nil, err
	}

	numbers, err := listSegments(path)
	if err != nil {
		return nil, err
	}

	var (
		number uint64
		f      *os.File
	)

	if len(numbers) == 0 {
		if hasManifest {
			return nil, fmt.Errorf(
				"%w: manifest checkpoint segment %d missing",
				ErrCorrupt,
				manifest.FirstSegment,
			)
		}

		number = 1

		f, err = os.OpenFile(
			segmentPath(path, number),
			os.O_RDWR|os.O_CREATE|os.O_EXCL,
			0644,
		)
		if err != nil {
			return nil, err
		}

		// Make the newly created segment durable.
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}

		ops := defaultPhysicalCompactionOps()

		if err := ops.syncDir(dir); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else {
		number = numbers[len(numbers)-1]

		f, err = os.OpenFile(
			segmentPath(path, number),
			os.O_RDWR,
			0644,
		)
		if err != nil {
			return nil, err
		}
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &SegmentedWAL{
		recovered: !hasManifest &&
			len(numbers) == 0,
		path:    path,
		options: options,
		active:  f,
		number:  number,
		size:    info.Size(),
	}, nil
}

func (w *SegmentedWAL) Close() error {
	if w.closed {
		return nil
	}

	w.closed = true

	if w.active == nil {
		return nil
	}

	return w.active.Close()
}
