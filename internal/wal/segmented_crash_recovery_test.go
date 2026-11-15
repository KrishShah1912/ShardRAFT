package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func createCrashRecoveryWAL(
	t *testing.T,
	path string,
	options SegmentOptions,
) *SegmentedWAL {
	t.Helper()

	w, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 20; i++ {
		if err := w.AppendEntries([]raft.Entry{{
			Index: i,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  []byte("command"),
		}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 20,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(10, 1); err != nil {
		t.Fatal(err)
	}

	return w
}

func verifyCrashRecoveryState(
	t *testing.T,
	path string,
	options SegmentOptions,
) {
	t.Helper()

	reopened, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if state.Snapshot.LastIncludedIndex != 10 {
		t.Fatalf(
			"snapshot index = %d, want 10",
			state.Snapshot.LastIncludedIndex,
		)
	}

	if state.Snapshot.LastIncludedTerm != 1 {
		t.Fatal("snapshot term lost")
	}

	if state.HardState.Commit != 20 {
		t.Fatal("commit index lost")
	}

	if len(state.Log) != 10 {
		t.Fatalf(
			"remaining log entries = %d, want 10",
			len(state.Log),
		)
	}

	if state.Log[0].Index != 11 {
		t.Fatal("remaining log starts at wrong index")
	}

	if state.Log[len(state.Log)-1].Index != 20 {
		t.Fatal("remaining log ends at wrong index")
	}
}

func TestSegmentedCrashBeforeCheckpointPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	temp, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}

	tempPath := temp.Name()

	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate interruption before the checkpoint is
	// renamed and the manifest is published.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// The abandoned temporary file may still exist.
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatal(err)
	}

	verifyCrashRecoveryState(t, path, options)
}

func TestSegmentedCrashAfterCheckpointPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	// Simulate restart before obsolete segments
	// have been retired.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	verifyCrashRecoveryState(t, path, options)
}

func TestSegmentedCrashAfterSegmentRetirement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	if err := w.RetireObsoleteSegments(); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	verifyCrashRecoveryState(t, path, options)
}
