package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func createRecoveryTestWAL(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("command-one")},
		{Index: 2, Term: 1, Data: []byte("command-two")},
	}); err != nil {
		w.Close()
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   1,
		Commit: 2,
	}); err != nil {
		w.Close()
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return path
}

func expectCorruptRecovery(t *testing.T, path string) {
	t.Helper()

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	_, err = w.Recover()

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 3, Term: 2},
	}); !errors.Is(err, ErrNotRecovered) {
		t.Fatalf(
			"expected ErrNotRecovered after failed recovery, got %v",
			err,
		)
	}
}

func TestRecoveryRejectsTruncatedHeader(t *testing.T) {
	path := createRecoveryTestWAL(t)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	expectCorruptRecovery(t, path)
}

func TestRecoveryRejectsTruncatedPayload(t *testing.T) {
	path := createRecoveryTestWAL(t)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[:4], 100)
	binary.LittleEndian.PutUint32(header[4:], 12345)

	if _, err := f.Write(header); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if _, err := f.Write([]byte("partial")); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	expectCorruptRecovery(t, path)
}

func TestRecoveryRejectsChecksumMismatch(t *testing.T) {
	path := createRecoveryTestWAL(t)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) <= headerSize {
		t.Fatal("WAL too small")
	}

	data[headerSize] ^= 0xFF

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	expectCorruptRecovery(t, path)
}

func TestRecoveryRejectsInvalidRecordLength(t *testing.T) {
	path := createRecoveryTestWAL(t)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	binary.LittleEndian.PutUint32(
		data[:4],
		uint32(maxRecord+1),
	)

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	expectCorruptRecovery(t, path)
}

func TestRecoveryRejectsUnknownRecordType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	payload, err := json.Marshal(record{
		Type: 255,
	})
	if err != nil {
		t.Fatal(err)
	}

	header := make([]byte, headerSize)

	binary.LittleEndian.PutUint32(
		header[:4],
		uint32(len(payload)),
	)

	binary.LittleEndian.PutUint32(
		header[4:],
		crc32.ChecksumIEEE(payload),
	)

	data := append(header, payload...)

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	expectCorruptRecovery(t, path)
}

func TestRecoveryRejectsEmptyRecordLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	header := make([]byte, headerSize)

	if err := os.WriteFile(path, header, 0644); err != nil {
		t.Fatal(err)
	}

	expectCorruptRecovery(t, path)
}

func TestRecoveryPreservesValidWAL(t *testing.T) {
	path := createRecoveryTestWAL(t)

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	state, err := w.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Log) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(state.Log))
	}

	if state.HardState.Term != 1 ||
		state.HardState.Commit != 2 {
		t.Fatalf("unexpected hard state: %+v", state.HardState)
	}
}
