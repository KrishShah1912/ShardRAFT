package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// readCheckpointFile validates a standalone checkpoint
// and reconstructs its persisted Raft state.
//
// A valid file is not necessarily published.
// Only MANIFEST determines publication.
func readCheckpointFile(
	path string,
) (raft.PersistentState, error) {
	var empty raft.PersistentState

	f, err := os.Open(path)
	if err != nil {
		return empty, err
	}
	defer f.Close()

	header := make([]byte, headerSize)

	if _, err := io.ReadFull(f, header); err != nil {
		return empty, fmt.Errorf(
			"%w: incomplete checkpoint header",
			ErrCorrupt,
		)
	}

	length := binary.LittleEndian.Uint32(header[:4])
	checksum := binary.LittleEndian.Uint32(header[4:])

	if length == 0 || length > maxRecord {
		return empty, fmt.Errorf(
			"%w: invalid checkpoint record length",
			ErrCorrupt,
		)
	}

	data := make([]byte, length)

	if _, err := io.ReadFull(f, data); err != nil {
		return empty, fmt.Errorf(
			"%w: incomplete checkpoint payload",
			ErrCorrupt,
		)
	}

	if crc32.ChecksumIEEE(data) != checksum {
		return empty, fmt.Errorf(
			"%w: checkpoint checksum mismatch",
			ErrCorrupt,
		)
	}

	var r record

	if err := json.Unmarshal(data, &r); err != nil {
		return empty, fmt.Errorf(
			"%w: invalid checkpoint JSON",
			ErrCorrupt,
		)
	}

	if r.Type != recordCheckpoint {
		return empty, fmt.Errorf(
			"%w: expected checkpoint record",
			ErrCorrupt,
		)
	}

	state, err := apply(empty, r)
	if err != nil {
		return empty, fmt.Errorf(
			"%w: invalid checkpoint state: %v",
			ErrCorrupt,
			err,
		)
	}

	var trailing [1]byte

	n, err := f.Read(trailing[:])
	if n != 0 || err != io.EOF {
		return empty, fmt.Errorf(
			"%w: trailing checkpoint data",
			ErrCorrupt,
		)
	}

	return copyState(state), nil
}
