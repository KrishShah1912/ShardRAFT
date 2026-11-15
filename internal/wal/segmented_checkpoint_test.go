package wal

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedCheckpointPreparation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(
		path,
		SegmentOptions{MaxSegmentSize: 256},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := uint64(1); i <= 10; i++ {
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
		Commit: 10,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(5, 1); err != nil {
		t.Fatal(err)
	}

	temp, err := w.prepareCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		name := temp.Name()
		_ = temp.Close()
		_ = os.Remove(name)
	}()

	header := make([]byte, headerSize)

	if _, err := io.ReadFull(temp, header); err != nil {
		t.Fatal(err)
	}

	length := binary.LittleEndian.Uint32(header[:4])
	checksum := binary.LittleEndian.Uint32(header[4:])

	data := make([]byte, length)

	if _, err := io.ReadFull(temp, data); err != nil {
		t.Fatal(err)
	}

	if crc32.ChecksumIEEE(data) != checksum {
		t.Fatal("checkpoint checksum mismatch")
	}

	var checkpoint record

	if err := json.Unmarshal(data, &checkpoint); err != nil {
		t.Fatal(err)
	}

	if checkpoint.Type != recordCheckpoint {
		t.Fatal("expected checkpoint record")
	}

	if checkpoint.State == nil {
		t.Fatal("missing checkpoint state")
	}

	if checkpoint.State.Snapshot.LastIncludedIndex != 5 {
		t.Fatal("incorrect snapshot boundary")
	}

	if len(checkpoint.State.Log) != 5 {
		t.Fatal("incorrect remaining log entries")
	}
}
