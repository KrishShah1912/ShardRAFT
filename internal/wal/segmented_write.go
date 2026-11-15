package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// writeRecord appends a checksummed record to the active segment.
// It synchronizes the record before updating in-memory Raft state.
func (w *SegmentedWAL) writeRecord(r record) error {
	if w.closed || w.active == nil || w.unusable {
		return ErrUnusable
	}
	if !w.recovered {
		return ErrNotRecovered
	}

	next, err := apply(w.state, r)
	if err != nil {
		return err
	}

	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > maxRecord {
		return fmt.Errorf("%w: invalid record size", ErrInvalid)
	}

	recordSize := int64(headerSize + len(data))

	if w.size > 0 &&
		w.size+recordSize > w.options.MaxSegmentSize {
		if err := w.rotate(); err != nil {
			w.unusable = true
			return err
		}
	}

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(
		header[4:], crc32.ChecksumIEEE(data),
	)

	if _, err := w.active.Seek(0, io.SeekEnd); err != nil {
		w.unusable = true
		return err
	}
	if err := writeFull(w.active, header); err != nil {
		w.unusable = true
		return err
	}
	if w.afterHeaderWrite != nil {
		if err := w.afterHeaderWrite(); err != nil {
			w.unusable = true
			return err
		}
	}
	if err := writeFull(w.active, data); err != nil {
		w.unusable = true
		return err
	}
	if err := w.active.Sync(); err != nil {
		w.unusable = true
		return err
	}

	w.size += recordSize
	w.state = next
	return nil
}

// rotate creates the next segment after syncing the current one.
func (w *SegmentedWAL) rotate() error {
	if err := w.active.Sync(); err != nil {
		return err
	}

	nextNumber := w.number + 1
	nextPath := segmentPath(w.path, nextNumber)

	f, err := os.OpenFile(
		nextPath,
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0644,
	)
	if err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	dir, err := os.Open(filepath.Dir(nextPath))
	if err != nil {
		f.Close()
		return err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		f.Close()
		return err
	}
	dir.Close()

	if err := w.active.Close(); err != nil {
		f.Close()
		return err
	}

	w.active = f
	w.number = nextNumber
	w.size = 0
	return nil
}

func (w *SegmentedWAL) AppendEntries(entries []raft.Entry) error {
	if len(entries) == 0 {
		if w.closed || w.unusable || w.active == nil {
			return ErrUnusable
		}
		if !w.recovered {
			return ErrNotRecovered
		}
		return nil
	}

	return w.writeRecord(record{
		Type:    recordEntries,
		Entries: copyEntries(entries),
	})
}

func (w *SegmentedWAL) ReplaceEntries(
	fromIndex uint64,
	entries []raft.Entry,
) error {
	return w.writeRecord(record{
		Type:      recordReplace,
		FromIndex: fromIndex,
		Entries:   copyEntries(entries),
	})
}

func (w *SegmentedWAL) TruncateSuffix(fromIndex uint64) error {
	return w.writeRecord(record{
		Type:      recordTruncate,
		FromIndex: fromIndex,
	})
}

func (w *SegmentedWAL) SaveHardState(hs raft.HardState) error {
	return w.writeRecord(record{
		Type:      recordHardState,
		HardState: &hs,
	})
}

func (w *SegmentedWAL) CompactPrefix(index, term uint64) error {
	return w.writeRecord(record{
		Type: recordCompaction,
		Snapshot: &raft.SnapshotMetadata{
			LastIncludedIndex: index,
			LastIncludedTerm:  term,
		},
	})
}

func (w *SegmentedWAL) Sync() error {
	if w.closed || w.active == nil || w.unusable {
		return ErrUnusable
	}
	if !w.recovered {
		return ErrNotRecovered
	}
	if err := w.active.Sync(); err != nil {
		w.unusable = true
		return err
	}
	return nil
}
