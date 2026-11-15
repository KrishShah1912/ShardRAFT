package wal

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedCheckpointPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

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

	oldNumber := w.number

	if err := w.PublishCheckpoint(); err != nil {
		t.Fatal(err)
	}

	if w.number != oldNumber+1 {
		t.Fatalf("checkpoint segment = %d", w.number)
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("missing published manifest")
	}

	if manifest.FirstSegment != w.number {
		t.Fatal("manifest points to wrong segment")
	}

	if err := w.VerifyPublishedCheckpoint(); err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{{
		Index: 21,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("after-checkpoint"),
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

	if len(state.Log) != 11 {
		t.Fatalf("remaining log entries = %d", len(state.Log))
	}

	if state.Log[len(state.Log)-1].Index != 21 {
		t.Fatal("post-checkpoint append missing")
	}
}
