package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedRecoveryDoesNotAppendToOrphanCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)

	originalNumber := w.number

	temp, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}

	tempPath := temp.Name()

	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}

	orphanNumber := originalNumber + 1

	if err := os.Rename(
		tempPath,
		segmentPath(path, orphanNumber),
	); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}

	// Recovery must not retain the orphan as the
	// active append target.
	if reopened.number == orphanNumber {
		t.Fatalf(
			"orphan segment %d selected as active writer",
			orphanNumber,
		)
	}

	if err := reopened.AppendEntries([]raft.Entry{{
		Index: 21,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("after-recovery"),
	}}); err != nil {
		t.Fatal(err)
	}

	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	final, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer final.Close()

	state, err := final.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Log) != 11 {
		t.Fatalf(
			"recovered %d entries, want 11",
			len(state.Log),
		)
	}

	if state.Log[len(state.Log)-1].Index != 21 {
		t.Fatal("post-recovery append was lost")
	}
}
