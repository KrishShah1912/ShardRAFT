package snapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const CurrentVersion = 1

var (
	ErrCorrupt = errors.New("corrupt snapshot")
	ErrInvalid = errors.New("invalid snapshot")
)

type Snapshot struct {
	Version      int                    `json:"version"`
	AppliedIndex uint64                 `json:"applied_index"`
	AppliedTerm  uint64                 `json:"applied_term"`
	Data         map[string]string      `json:"data"`
	Dedup        map[string]DedupRecord `json:"dedup,omitempty"`
}

type envelope struct {
	Snapshot Snapshot `json:"snapshot"`
	Checksum string   `json:"checksum"`
}

// New constructs a snapshot with a copy of the KV data.
func New(
	appliedIndex uint64,
	appliedTerm uint64,
	data map[string]string,
) Snapshot {
	copied := make(map[string]string, len(data))

	for key, value := range data {
		copied[key] = value
	}

	return Snapshot{
		Version:      CurrentVersion,
		AppliedIndex: appliedIndex,
		AppliedTerm:  appliedTerm,
		Data:         copied,
	}
}

func (s Snapshot) Validate() error {
	if s.Version != CurrentVersion {
		return fmt.Errorf(
			"%w: unsupported version %d",
			ErrInvalid,
			s.Version,
		)
	}

	if s.AppliedIndex == 0 && s.AppliedTerm != 0 {
		return fmt.Errorf(
			"%w: term without applied index",
			ErrInvalid,
		)
	}

	if s.AppliedIndex > 0 && s.AppliedTerm == 0 {
		return fmt.Errorf(
			"%w: missing applied term",
			ErrInvalid,
		)
	}

	if s.Data == nil {
		return fmt.Errorf(
			"%w: nil KV data",
			ErrInvalid,
		)
	}

	return nil
}

// Save atomically writes a snapshot.
//
// It writes to a temporary file, fsyncs the file,
// renames it into place, then syncs the directory.
func Save(path string, s Snapshot) error {
	if err := s.Validate(); err != nil {
		return err
	}

	payload, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	sum := sha256.Sum256(payload)

	wrapped := envelope{
		Snapshot: s,
		Checksum: fmt.Sprintf("%x", sum),
	}

	encoded, err := json.Marshal(wrapped)
	if err != nil {
		return fmt.Errorf("encode snapshot envelope: %w", err)
	}

	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}

	file, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return fmt.Errorf("create temporary snapshot: %w", err)
	}

	tempPath := file.Name()
	defer os.Remove(tempPath)

	if _, err := file.Write(encoded); err != nil {
		file.Close()
		return fmt.Errorf("write snapshot: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync snapshot: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace snapshot: %w", err)
	}

	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open snapshot directory: %w", err)
	}
	defer directory.Close()

	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}

	return nil
}

// Load reads and verifies a saved snapshot.
func Load(path string) (Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}

	var wrapped envelope

	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return Snapshot{}, fmt.Errorf(
			"%w: invalid JSON: %v",
			ErrCorrupt,
			err,
		)
	}

	payload, err := json.Marshal(wrapped.Snapshot)
	if err != nil {
		return Snapshot{}, fmt.Errorf(
			"%w: invalid payload: %v",
			ErrCorrupt,
			err,
		)
	}

	expected := sha256.Sum256(payload)
	actual := fmt.Sprintf("%x", expected)

	if !bytes.Equal(
		[]byte(actual),
		[]byte(wrapped.Checksum),
	) {
		return Snapshot{}, fmt.Errorf(
			"%w: checksum mismatch",
			ErrCorrupt,
		)
	}

	if err := wrapped.Snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}

	return wrapped.Snapshot, nil
}
