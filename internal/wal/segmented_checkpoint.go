package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"reflect"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// prepareCheckpoint creates a durable temporary checkpoint.
//
// It does not change active WAL segments.
// The caller owns the returned file and must remove it
// after use.
//
// All WAL mutations must be serialized by the caller.
func (w *SegmentedWAL) prepareCheckpoint() (*os.File, error) {
	if w.closed || w.active == nil || w.unusable {
		return nil, ErrUnusable
	}

	if !w.recovered {
		return nil, ErrNotRecovered
	}

	if w.state.Snapshot.LastIncludedIndex == 0 {
		return nil, fmt.Errorf(
			"%w: no compacted snapshot boundary",
			ErrInvalid,
		)
	}

	state := copyState(w.state)

	checkpoint := record{
		Type:  recordCheckpoint,
		State: &state,
	}

	reconstructed, err := apply(
		raft.PersistentState{},
		checkpoint,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"validate checkpoint: %w", err,
		)
	}

	if !reflect.DeepEqual(
		copyState(reconstructed),
		copyState(state),
	) {
		return nil, fmt.Errorf(
			"%w: checkpoint state mismatch",
			ErrInvalid,
		)
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 || len(data) > maxRecord {
		return nil, fmt.Errorf(
			"%w: invalid checkpoint size",
			ErrInvalid,
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

	dir := segmentDirectory(w.path)

	temp, err := os.CreateTemp(
		dir,
		".checkpoint-*",
	)
	if err != nil {
		return nil, err
	}

	fail := func(err error) (*os.File, error) {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
		return nil, err
	}

	if err := writeFull(temp, header); err != nil {
		return fail(err)
	}

	if err := writeFull(temp, data); err != nil {
		return fail(err)
	}

	if err := temp.Sync(); err != nil {
		return fail(err)
	}

	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}

	checkHeader := make([]byte, headerSize)

	if _, err := io.ReadFull(temp, checkHeader); err != nil {
		return fail(err)
	}

	length := binary.LittleEndian.Uint32(checkHeader[:4])
	checksum := binary.LittleEndian.Uint32(checkHeader[4:])

	if length != uint32(len(data)) ||
		checksum != crc32.ChecksumIEEE(data) {
		return fail(fmt.Errorf(
			"%w: checkpoint header mismatch",
			ErrCorrupt,
		))
	}

	checkData := make([]byte, length)

	if _, err := io.ReadFull(temp, checkData); err != nil {
		return fail(err)
	}

	if crc32.ChecksumIEEE(checkData) != checksum {
		return fail(fmt.Errorf(
			"%w: checkpoint checksum mismatch",
			ErrCorrupt,
		))
	}

	var verified record

	if err := json.Unmarshal(checkData, &verified); err != nil {
		return fail(fmt.Errorf(
			"%w: invalid checkpoint JSON: %v",
			ErrCorrupt,
			err,
		))
	}

	verifiedState, err := apply(
		raft.PersistentState{},
		verified,
	)
	if err != nil {
		return fail(err)
	}

	if !reflect.DeepEqual(
		copyState(verifiedState),
		copyState(state),
	) {
		return fail(fmt.Errorf(
			"%w: checkpoint verification mismatch",
			ErrCorrupt,
		))
	}

	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}

	return temp, nil
}
