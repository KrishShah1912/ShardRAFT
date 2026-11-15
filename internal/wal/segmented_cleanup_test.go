package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedRetireObsoleteSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

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

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("missing manifest")
	}

	if manifest.FirstSegment <= 1 {
		t.Fatal("expected checkpoint generation after segment 1")
	}

	if err := w.RetireObsoleteSegments(); err != nil {
		t.Fatal(err)
	}

	for number := uint64(1); number < manifest.FirstSegment; number++ {
		if _, err := os.Stat(segmentPath(path, number)); !os.IsNotExist(err) {
			t.Fatalf(
				"obsolete segment %d still exists: %v",
				number,
				err,
			)
		}
	}

	if _, err := os.Stat(
		segmentPath(path, manifest.FirstSegment),
	); err != nil {
		t.Fatalf("active checkpoint missing: %v", err)
	}

	if err := w.AppendEntries([]raft.Entry{{
		Index: 21,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("after-cleanup"),
	}}); err != nil {
		t.Fatal(err)
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

	if state.Snapshot.LastIncludedIndex != 10 {
		t.Fatal("snapshot boundary lost")
	}

	if state.HardState.Commit != 20 {
		t.Fatal("commit index lost")
	}

	if len(state.Log) != 11 {
		t.Fatalf("log entries = %d, want 11", len(state.Log))
	}

	if state.Log[len(state.Log)-1].Index != 21 {
		t.Fatal("post-cleanup append missing")
	}
}
