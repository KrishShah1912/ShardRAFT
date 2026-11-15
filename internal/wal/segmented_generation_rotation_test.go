package wal

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedRecoveryPreservesEntriesAfterManifestBoundary(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("manifest missing")
	}

	// Record the checkpoint generation boundary.
	manifest.LastSegment = w.number

	if err := writeSegmentedManifest(path, manifest); err != nil {
		t.Fatal(err)
	}

	checkpointNumber := w.number

	// Append enough entries to force rotation beyond
	// the published checkpoint segment.
	for i := uint64(21); i <= 30; i++ {
		if err := w.AppendEntries([]raft.Entry{{
			Index: i,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  []byte("after-checkpoint"),
		}}); err != nil {
			t.Fatal(err)
		}
	}

	if w.number <= checkpointNumber {
		t.Fatal("expected WAL rotation after checkpoint")
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Log) != 20 {
		t.Fatalf(
			"recovered %d entries, want 20",
			len(state.Log),
		)
	}

	if state.Log[0].Index != 11 {
		t.Fatal("recovered log starts at wrong index")
	}

	if state.Log[len(state.Log)-1].Index != 30 {
		t.Fatal("entries after checkpoint were lost")
	}
}
