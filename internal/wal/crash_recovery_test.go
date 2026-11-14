package wal

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// TestWALRejectsPartialTrailingHeader verifies that
// recovery rejects an incomplete record header.
func TestWALRejectsPartialTrailingHeader(t *testing.T) {
	w, path := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("first")},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash that leaves only part
	// of the next record header.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte{0x10, 0x00, 0x00}); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	_, err = reopened.Recover()

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}

	// Recovery must not enable further writes
	// after encountering an incomplete record.
	err = reopened.AppendEntries([]raft.Entry{
		{Index: 2, Term: 1},
	})

	if !errors.Is(err, ErrNotRecovered) {
		t.Fatalf(
			"expected ErrNotRecovered, got %v",
			err,
		)
	}
}

// TestWALRejectsPartialTrailingPayload verifies
// that valid earlier records do not cause recovery
// to ignore an incomplete final record.
func TestWALRejectsPartialTrailingPayload(t *testing.T) {
	w, path := openTestWAL(t)

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("committed")},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Construct a record header claiming a
	// 100-byte payload, but write only 4 bytes.
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[:4], 100)
	binary.LittleEndian.PutUint32(header[4:], 0)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write(header); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if _, err := f.Write([]byte("test")); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	_, err = reopened.Recover()

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

// TestWALRejectsCorruptionAfterCompaction verifies
// that recovery does not silently accept corrupted
// records following a valid compaction boundary.
func TestWALRejectsCorruptionAfterCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compacted.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   2,
		Commit: 3,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.CompactPrefix(2, 1); err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 4, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the last record.
	// The checksum must detect this change.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	if info.Size() < 1 {
		f.Close()
		t.Fatal("WAL is unexpectedly empty")
	}

	last := make([]byte, 1)

	if _, err := f.ReadAt(last, info.Size()-1); err != nil {
		f.Close()
		t.Fatal(err)
	}

	last[0] ^= 0xFF

	if _, err := f.WriteAt(last, info.Size()-1); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	_, err = reopened.Recover()

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf(
			"expected ErrCorrupt after compaction, got %v",
			err,
		)
	}
}
