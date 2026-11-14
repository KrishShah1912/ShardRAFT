package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

var errInjectedWALIO = errors.New("injected WAL I/O failure")

// failureFile wraps a real file and simulates
// deterministic write, seek, and sync failures.
type failureFile struct {
	*os.File

	failWriteAt    int
	writeCount     int
	failSync       bool
	failSeek       bool
	partialPayload bool
}

func (f *failureFile) Write(p []byte) (int, error) {
	f.writeCount++

	if f.partialPayload && f.writeCount == 2 {
		n := len(p) / 2

		if n == 0 {
			return 0, errInjectedWALIO
		}

		written, err := f.File.Write(p[:n])
		if err != nil {
			return written, err
		}

		return written, errInjectedWALIO
	}

	if f.failWriteAt > 0 && f.writeCount == f.failWriteAt {
		return 0, errInjectedWALIO
	}

	return f.File.Write(p)
}

func (f *failureFile) Sync() error {
	if f.failSync {
		return errInjectedWALIO
	}

	return f.File.Sync()
}

func (f *failureFile) Seek(offset int64, whence int) (int64, error) {
	if f.failSeek {
		return 0, errInjectedWALIO
	}

	return f.File.Seek(offset, whence)
}

func newInjectedWAL(t *testing.T) (*WAL, string, *failureFile) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "injected.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = w.Close()
	})

	f := &failureFile{
		File: w.file.(*os.File),
	}

	w.file = f

	return w, path, f
}

func assertInjectedWALUnusable(t *testing.T, w *WAL) {
	t.Helper()

	if !w.unusable {
		t.Fatal("WAL was not marked unusable")
	}

	if len(w.state.Log) != 0 {
		t.Fatalf(
			"state advanced despite failure: %+v",
			w.state.Log,
		)
	}

	if err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	}); !errors.Is(err, ErrUnusable) {
		t.Fatalf(
			"append after failure: expected ErrUnusable, got %v",
			err,
		)
	}

	if err := w.Sync(); !errors.Is(err, ErrUnusable) {
		t.Fatalf(
			"sync after failure: expected ErrUnusable, got %v",
			err,
		)
	}

	if _, err := w.Recover(); !errors.Is(err, ErrUnusable) {
		t.Fatalf(
			"recover on unusable handle: expected ErrUnusable, got %v",
			err,
		)
	}
}

// Test 1: Failure while writing the WAL header.
func TestWALInjectedHeaderWriteFailure(t *testing.T) {
	w, _, f := newInjectedWAL(t)

	f.failWriteAt = 1

	err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	})

	if !errors.Is(err, errInjectedWALIO) {
		t.Fatalf("expected injected failure, got %v", err)
	}

	assertInjectedWALUnusable(t, w)
}

// Test 2: Failure while writing the WAL payload.
func TestWALInjectedPayloadWriteFailure(t *testing.T) {
	w, _, f := newInjectedWAL(t)

	f.failWriteAt = 2

	err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	})

	if !errors.Is(err, errInjectedWALIO) {
		t.Fatalf("expected injected failure, got %v", err)
	}

	assertInjectedWALUnusable(t, w)
}

// Test 3: Failure while seeking to the end of the WAL.
func TestWALInjectedSeekFailure(t *testing.T) {
	w, _, f := newInjectedWAL(t)

	f.failSeek = true

	err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	})

	if !errors.Is(err, errInjectedWALIO) {
		t.Fatalf("expected injected failure, got %v", err)
	}

	assertInjectedWALUnusable(t, w)
}

// Test 4: Failure during record synchronization.
func TestWALInjectedRecordSyncFailure(t *testing.T) {
	w, _, f := newInjectedWAL(t)

	f.failSync = true

	err := w.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	})

	if !errors.Is(err, errInjectedWALIO) {
		t.Fatalf("expected injected failure, got %v", err)
	}

	assertInjectedWALUnusable(t, w)
}

// Test 5: Failure during an explicit Sync call.
func TestWALInjectedExplicitSyncFailure(t *testing.T) {
	w, _, f := newInjectedWAL(t)

	f.failSync = true

	err := w.Sync()

	if !errors.Is(err, errInjectedWALIO) {
		t.Fatalf("expected injected failure, got %v", err)
	}

	assertInjectedWALUnusable(t, w)
}

// Test 6: Partial payload write followed by recovery.
func TestWALInjectedPartialPayloadRejectsRecovery(t *testing.T) {
	w, path, f := newInjectedWAL(t)

	f.partialPayload = true

	err := w.AppendEntries([]raft.Entry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("SET x=10"),
		},
	})

	if !errors.Is(err, errInjectedWALIO) {
		t.Fatalf("expected injected failure, got %v", err)
	}

	assertInjectedWALUnusable(t, w)

	if err := w.Close(); err != nil {
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
			"expected ErrCorrupt for partial record, got %v",
			err,
		)
	}

	err = reopened.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1},
	})

	if !errors.Is(err, ErrNotRecovered) {
		t.Fatalf(
			"expected ErrNotRecovered after failed recovery, got %v",
			err,
		)
	}
}
