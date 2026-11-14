package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

const (
	recordEntries   = 1
	recordHardState = 2
	recordReplace   = 3
	recordTruncate  = 4

	headerSize = 8
	maxRecord  = 64 << 20
)

var (
	ErrCorrupt      = errors.New("corrupt WAL record")
	ErrInvalid      = errors.New("invalid WAL operation")
	ErrUnusable     = errors.New("WAL is unusable")
	ErrNotRecovered = errors.New("existing WAL must be recovered before writing")
)

type record struct {
	Type      uint8           `json:"type"`
	FromIndex uint64          `json:"from_index,omitempty"`
	Entries   []raft.Entry    `json:"entries,omitempty"`
	HardState *raft.HardState `json:"hard_state,omitempty"`
}

type WAL struct {
	file      *os.File
	state     raft.PersistentState
	recovered bool
	unusable  bool
	closed    bool
}

func Open(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &WAL{
		file:      f,
		recovered: info.Size() == 0,
	}, nil
}

func copyEntries(entries []raft.Entry) []raft.Entry {
	result := make([]raft.Entry, len(entries))

	for i, entry := range entries {
		result[i] = entry
		result[i].Data = append([]byte(nil), entry.Data...)
	}

	return result
}

func copyState(s raft.PersistentState) raft.PersistentState {
	return raft.PersistentState{
		HardState: s.HardState,
		Log:       copyEntries(s.Log),
	}
}

func apply(
	current raft.PersistentState,
	r record,
) (raft.PersistentState, error) {
	next := copyState(current)

	switch r.Type {
	case recordEntries:
		if len(r.Entries) == 0 {
			return current, fmt.Errorf(
				"%w: empty append", ErrInvalid,
			)
		}

		for _, entry := range r.Entries {
			expected := uint64(len(next.Log)) + 1

			if entry.Index != expected || entry.Term == 0 {
				return current, fmt.Errorf(
					"%w: invalid entry at index %d",
					ErrInvalid, entry.Index,
				)
			}

			if len(next.Log) > 0 &&
				entry.Term < next.Log[len(next.Log)-1].Term {
				return current, fmt.Errorf(
					"%w: log term regression", ErrInvalid,
				)
			}

			next.Log = append(
				next.Log,
				copyEntries([]raft.Entry{entry})...,
			)
		}

	case recordReplace:
		if r.FromIndex == 0 || len(r.Entries) == 0 {
			return current, fmt.Errorf(
				"%w: invalid replacement", ErrInvalid,
			)
		}

		if r.FromIndex <= next.HardState.Commit {
			return current, fmt.Errorf(
				"%w: cannot replace committed entries",
				ErrInvalid,
			)
		}

		if r.FromIndex > uint64(len(next.Log))+1 {
			return current, fmt.Errorf(
				"%w: replacement creates a gap",
				ErrInvalid,
			)
		}

		next.Log = next.Log[:r.FromIndex-1]

		for _, entry := range r.Entries {
			expected := uint64(len(next.Log)) + 1

			if entry.Index != expected || entry.Term == 0 {
				return current, fmt.Errorf(
					"%w: invalid replacement entry",
					ErrInvalid,
				)
			}

			if len(next.Log) > 0 &&
				entry.Term < next.Log[len(next.Log)-1].Term {
				return current, fmt.Errorf(
					"%w: replacement term regression",
					ErrInvalid,
				)
			}

			next.Log = append(
				next.Log,
				copyEntries([]raft.Entry{entry})...,
			)
		}

	case recordTruncate:
		if r.FromIndex == 0 {
			return current, fmt.Errorf(
				"%w: invalid truncation index",
				ErrInvalid,
			)
		}

		if r.FromIndex <= next.HardState.Commit {
			return current, fmt.Errorf(
				"%w: cannot truncate committed entries",
				ErrInvalid,
			)
		}

		if r.FromIndex > uint64(len(next.Log))+1 {
			return current, fmt.Errorf(
				"%w: truncation index exceeds log",
				ErrInvalid,
			)
		}

		if len(r.Entries) != 0 {
			return current, fmt.Errorf(
				"%w: truncation cannot append entries",
				ErrInvalid,
			)
		}

		next.Log = next.Log[:r.FromIndex-1]

	case recordHardState:
		if r.HardState == nil {
			return current, fmt.Errorf(
				"%w: missing hard state", ErrInvalid,
			)
		}

		hs := *r.HardState
		old := next.HardState

		if hs.Term < old.Term {
			return current, fmt.Errorf(
				"%w: term regression", ErrInvalid,
			)
		}

		if hs.Commit < old.Commit {
			return current, fmt.Errorf(
				"%w: commit regression", ErrInvalid,
			)
		}

		if hs.Commit > uint64(len(next.Log)) {
			return current, fmt.Errorf(
				"%w: commit exceeds log", ErrInvalid,
			)
		}

		if hs.Term == old.Term && hs.Vote != old.Vote {
			if old.Vote != 0 {
				return current, fmt.Errorf(
					"%w: vote changed in same term",
					ErrInvalid,
				)
			}
		}

		if hs.Term == 0 && hs.Vote != 0 {
			return current, fmt.Errorf(
				"%w: vote without term", ErrInvalid,
			)
		}

		next.HardState = hs

	default:
		return current, fmt.Errorf(
			"%w: unknown record type", ErrInvalid,
		)
	}

	return next, nil
}

func (w *WAL) checkWritable() error {
	if w.closed || w.unusable {
		return ErrUnusable
	}
	if !w.recovered {
		return ErrNotRecovered
	}
	return nil
}

func (w *WAL) writeRecord(r record) error {
	if err := w.checkWritable(); err != nil {
		return err
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
		return fmt.Errorf(
			"%w: invalid record size", ErrInvalid,
		)
	}

	header := make([]byte, headerSize)

	binary.LittleEndian.PutUint32(
		header[:4],
		uint32(len(data)),
	)

	binary.LittleEndian.PutUint32(
		header[4:],
		crc32.ChecksumIEEE(data),
	)

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		w.unusable = true
		return err
	}

	if err := writeFull(w.file, header); err != nil {
		w.unusable = true
		return err
	}

	if err := writeFull(w.file, data); err != nil {
		w.unusable = true
		return err
	}

	if err := w.file.Sync(); err != nil {
		w.unusable = true
		return err
	}

	w.state = next
	return nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}

		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}

		data = data[n:]
	}

	return nil
}

func (w *WAL) AppendEntries(entries []raft.Entry) error {
	if len(entries) == 0 {
		return w.checkWritable()
	}

	return w.writeRecord(record{
		Type:    recordEntries,
		Entries: copyEntries(entries),
	})
}

func (w *WAL) ReplaceEntries(
	fromIndex uint64,
	entries []raft.Entry,
) error {
	return w.writeRecord(record{
		Type:      recordReplace,
		FromIndex: fromIndex,
		Entries:   copyEntries(entries),
	})
}

// TruncateSuffix durably removes an uncommitted log suffix.
func (w *WAL) TruncateSuffix(fromIndex uint64) error {
	return w.writeRecord(record{
		Type:      recordTruncate,
		FromIndex: fromIndex,
	})
}

func (w *WAL) SaveHardState(hs raft.HardState) error {
	return w.writeRecord(record{
		Type:      recordHardState,
		HardState: &hs,
	})
}

func (w *WAL) Sync() error {
	if err := w.checkWritable(); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		w.unusable = true
		return err
	}

	return nil
}

func (w *WAL) Recover() (raft.PersistentState, error) {
	var state raft.PersistentState

	if w.closed || w.unusable {
		return state, ErrUnusable
	}

	w.recovered = false

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		w.unusable = true
		return state, err
	}

	for {
		header := make([]byte, headerSize)

		_, err := io.ReadFull(w.file, header)

		if err == io.EOF {
			break
		}

		if err != nil {
			return state, fmt.Errorf(
				"%w: incomplete header", ErrCorrupt,
			)
		}

		length := binary.LittleEndian.Uint32(header[:4])
		checksum := binary.LittleEndian.Uint32(header[4:])

		if length == 0 || length > maxRecord {
			return state, fmt.Errorf(
				"%w: invalid record length", ErrCorrupt,
			)
		}

		data := make([]byte, length)

		if _, err := io.ReadFull(w.file, data); err != nil {
			return state, fmt.Errorf(
				"%w: incomplete payload", ErrCorrupt,
			)
		}

		if crc32.ChecksumIEEE(data) != checksum {
			return state, fmt.Errorf(
				"%w: checksum mismatch", ErrCorrupt,
			)
		}

		var r record

		if err := json.Unmarshal(data, &r); err != nil {
			return state, fmt.Errorf(
				"%w: invalid JSON", ErrCorrupt,
			)
		}

		next, err := apply(state, r)
		if err != nil {
			return state, fmt.Errorf(
				"%w: %v", ErrCorrupt, err,
			)
		}

		state = next
	}

	w.state = copyState(state)
	w.recovered = true

	return copyState(state), nil
}

func (w *WAL) Close() error {
	if w.closed {
		return nil
	}

	w.closed = true
	return w.file.Close()
}
