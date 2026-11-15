package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedWALRotates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(
		path,
		SegmentOptions{MaxSegmentSize: 256},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := uint64(1); i <= 20; i++ {
		err := w.AppendEntries([]raft.Entry{{
			Index: i,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  []byte("some test payload"),
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	numbers, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(numbers) < 2 {
		t.Fatalf(
			"expected multiple segments, got %d",
			len(numbers),
		)
	}

	for _, number := range numbers {
		info, err := os.Stat(segmentPath(path, number))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() == 0 {
			t.Fatalf("segment %d is empty", number)
		}
	}
}
