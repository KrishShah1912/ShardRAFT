package shard

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

func recoveryTestConfig(path string) Config {
	return Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         path,
		Send: func([]raft.Message) error {
			return nil
		},
	}
}

func createCompactedRecoveryFixture(
	t *testing.T,
	path string,
	snapshotPath string,
) {
	t.Helper()

	storage, err := wal.OpenSegmented(
		path, wal.SegmentOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := storage.Recover(); err != nil {
		t.Fatal(err)
	}

	if err := storage.SaveHardState(raft.HardState{
		Term: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if err := storage.AppendEntries([]raft.Entry{{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNoop,
	}}); err != nil {
		t.Fatal(err)
	}

	if err := storage.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if err := snapshot.Save(
		snapshotPath,
		snapshot.NewWithDedup(
			1,
			1,
			map[string]string{"restored": "yes"},
			nil,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := storage.CompactPrefix(1, 1); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShardRecoversCompactedSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.wal")
	snapshotPath := path + ".snapshot"

	createCompactedRecoveryFixture(t, path, snapshotPath)

	s, err := Open(recoveryTestConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	value, exists, err := s.Get("restored")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || value != "yes" {
		t.Fatalf(
			"restored value = %q, exists=%v",
			value, exists,
		)
	}

	index, err := s.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Fatalf("applied index = %d, want 1", index)
	}
}

func TestShardRejectsMissingCompactedSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.wal")
	snapshotPath := path + ".snapshot"

	createCompactedRecoveryFixture(t, path, snapshotPath)

	if err := os.Remove(snapshotPath); err != nil {
		t.Fatal(err)
	}

	s, err := Open(recoveryTestConfig(path))
	if err == nil {
		_ = s.Close()
		t.Fatal("expected missing snapshot rejection")
	}
}

func TestShardRejectsCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.wal")
	snapshotPath := path + ".snapshot"

	createCompactedRecoveryFixture(t, path, snapshotPath)

	if err := os.WriteFile(
		snapshotPath,
		[]byte("corrupted snapshot"),
		0600,
	); err != nil {
		t.Fatal(err)
	}

	s, err := Open(recoveryTestConfig(path))
	if err == nil {
		_ = s.Close()
		t.Fatal("expected corrupt snapshot rejection")
	}
	if !errors.Is(err, snapshot.ErrCorrupt) {
		t.Fatalf("expected snapshot.ErrCorrupt, got %v", err)
	}
}

func TestShardRejectsSnapshotTermMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.wal")
	snapshotPath := path + ".snapshot"

	createCompactedRecoveryFixture(t, path, snapshotPath)

	if err := snapshot.Save(
		snapshotPath,
		snapshot.New(1, 2, map[string]string{
			"restored": "yes",
		}),
	); err != nil {
		t.Fatal(err)
	}

	s, err := Open(recoveryTestConfig(path))
	if err == nil {
		_ = s.Close()
		t.Fatal("expected snapshot term mismatch rejection")
	}
}
