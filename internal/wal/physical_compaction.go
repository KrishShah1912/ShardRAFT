package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// RewriteCheckpoint physically rewrites the WAL as a
// single checkpoint containing the current persisted state.
//
// The caller must serialize this operation with every
// other Raft and WAL mutation.
//
// A durable, matching KV snapshot must already exist
// before historical WAL entries are discarded.
//
// This method must receive the same path used by Open.
// It must not run concurrently with another WAL opener
// or writer.
//
// After an error, the caller should stop using this WAL,
// close it, and recover from the on-disk WAL.
func (w *WAL) RewriteCheckpoint(path string) error {
	if err := w.checkWritable(); err != nil {
		return err
	}

	if path == "" {
		return fmt.Errorf(
			"%w: empty WAL path",
			ErrInvalid,
		)
	}

	original, ok := w.file.(*os.File)
	if !ok {
		return fmt.Errorf(
			"%w: physical rewrite requires a real file",
			ErrInvalid,
		)
	}

	// Verify that the supplied path identifies the
	// same file currently owned by this WAL.
	originalInfo, err := original.Stat()
	if err != nil {
		w.unusable = true
		return fmt.Errorf("stat original WAL: %w", err)
	}

	pathInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat WAL path: %w", err)
	}

	if !os.SameFile(originalInfo, pathInfo) {
		return fmt.Errorf(
			"%w: path does not match open WAL",
			ErrInvalid,
		)
	}

	if w.state.Snapshot.LastIncludedIndex == 0 {
		return fmt.Errorf(
			"%w: no compacted snapshot boundary",
			ErrInvalid,
		)
	}

	// Construct a checkpoint from the durable
	// in-memory WAL state.
	checkpointState := copyState(w.state)

	checkpoint := record{
		Type:  recordCheckpoint,
		State: &checkpointState,
	}

	// Validate the checkpoint against an empty state.
	reconstructed, err := apply(
		raft.PersistentState{},
		checkpoint,
	)
	if err != nil {
		return fmt.Errorf(
			"validate checkpoint: %w",
			err,
		)
	}

	if !reflect.DeepEqual(
		copyState(reconstructed),
		copyState(w.state),
	) {
		return fmt.Errorf(
			"%w: checkpoint state mismatch",
			ErrInvalid,
		)
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	if len(data) == 0 || len(data) > maxRecord {
		return fmt.Errorf(
			"%w: checkpoint exceeds WAL record limit",
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

	// Ensure the original WAL is durable before
	// beginning the replacement.
	if err := original.Sync(); err != nil {
		w.unusable = true
		return fmt.Errorf(
			"sync original WAL: %w",
			err,
		)
	}

	dir := filepath.Dir(path)

	// The temporary file is created in the same
	// directory to support atomic replacement.
	temp, err := os.CreateTemp(dir, ".wal-rewrite-*")
	if err != nil {
		return fmt.Errorf(
			"create temporary WAL: %w",
			err,
		)
	}

	tempPath := temp.Name()
	renamed := false
	tempOwned := true

	defer func() {
		if tempOwned {
			_ = temp.Close()
		}

		if !renamed {
			_ = os.Remove(tempPath)
		}
	}()

	// Preserve the original file permissions.
	if err := temp.Chmod(originalInfo.Mode().Perm()); err != nil {
		return fmt.Errorf(
			"set temporary WAL permissions: %w",
			err,
		)
	}

	// Write the complete checkpoint record.
	if err := writeFull(temp, header); err != nil {
		return fmt.Errorf(
			"write checkpoint header: %w",
			err,
		)
	}

	if err := writeFull(temp, data); err != nil {
		return fmt.Errorf(
			"write checkpoint payload: %w",
			err,
		)
	}

	if err := temp.Sync(); err != nil {
		return fmt.Errorf(
			"sync temporary WAL: %w",
			err,
		)
	}

	// Verify the checkpoint stored in the temporary
	// file before replacing the original WAL.
	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf(
			"seek temporary WAL: %w",
			err,
		)
	}

	checkHeader := make([]byte, headerSize)

	if _, err := io.ReadFull(temp, checkHeader); err != nil {
		return fmt.Errorf(
			"read checkpoint header: %w",
			err,
		)
	}

	length := binary.LittleEndian.Uint32(checkHeader[:4])
	checksum := binary.LittleEndian.Uint32(checkHeader[4:])

	if length != uint32(len(data)) ||
		checksum != crc32.ChecksumIEEE(data) {
		return fmt.Errorf(
			"%w: checkpoint header mismatch",
			ErrCorrupt,
		)
	}

	checkData := make([]byte, length)

	if _, err := io.ReadFull(temp, checkData); err != nil {
		return fmt.Errorf(
			"read checkpoint payload: %w",
			err,
		)
	}

	if crc32.ChecksumIEEE(checkData) != checksum {
		return fmt.Errorf(
			"%w: checkpoint checksum mismatch",
			ErrCorrupt,
		)
	}

	var verifiedRecord record

	if err := json.Unmarshal(
		checkData,
		&verifiedRecord,
	); err != nil {
		return fmt.Errorf(
			"%w: invalid checkpoint JSON: %v",
			ErrCorrupt,
			err,
		)
	}

	verifiedState, err := apply(
		raft.PersistentState{},
		verifiedRecord,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: invalid checkpoint: %v",
			ErrCorrupt,
			err,
		)
	}

	if !reflect.DeepEqual(
		copyState(verifiedState),
		copyState(w.state),
	) {
		return fmt.Errorf(
			"%w: checkpoint verification mismatch",
			ErrCorrupt,
		)
	}

	// The replacement file must be positioned at EOF
	// so subsequent appends remain valid.
	if _, err := temp.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf(
			"seek checkpoint end: %w",
			err,
		)
	}

	// Atomically replace the original WAL path.
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf(
			"replace WAL: %w",
			err,
		)
	}

	renamed = true

	// The original descriptor now refers to the old
	// file. Prevent further writes until the new
	// directory entry has been synchronized.
	w.unusable = true

	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf(
			"open WAL directory: %w",
			err,
		)
	}

	syncErr := directory.Sync()
	closeErr := directory.Close()

	if syncErr != nil {
		return fmt.Errorf(
			"sync WAL directory: %w",
			syncErr,
		)
	}

	if closeErr != nil {
		return fmt.Errorf(
			"close WAL directory: %w",
			closeErr,
		)
	}

	// Switch the WAL to the replacement descriptor.
	// The original descriptor must no longer be used.
	w.file = temp
	tempOwned = false

	if err := original.Close(); err != nil {
		return fmt.Errorf(
			"close original WAL: %w",
			err,
		)
	}

	w.recovered = true
	w.unusable = false

	return nil
}
