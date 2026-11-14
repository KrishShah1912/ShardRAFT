package wal

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// preparePhysicalCompactionFailureWAL creates a WAL
// containing committed entries and a compacted boundary.
func preparePhysicalCompactionFailureWAL(
	t *testing.T,
) (string, *WAL, raft.PersistentState) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []raft.Entry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 2, Data: []byte("three")},
		{Index: 4, Term: 2, Data: []byte("four")},
	}

	if err := w.AppendEntries(entries); err != nil {
		w.Close()
		t.Fatal(err)
	}

	if err := w.SaveHardState(raft.HardState{
		Term:   2,
		Commit: 3,
	}); err != nil {
		w.Close()
		t.Fatal(err)
	}

	if err := w.CompactPrefix(2, 1); err != nil {
		w.Close()
		t.Fatal(err)
	}

	return path, w, copyState(w.state)
}

// recoverPhysicalCompactionFailureWAL reopens the
// on-disk WAL and checks its recovered Raft state.
func recoverPhysicalCompactionFailureWAL(
	t *testing.T,
	path string,
	expected raft.PersistentState,
) {
	t.Helper()

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	actual, err := reopened.Recover()
	if err != nil {
		t.Fatalf("recover WAL: %v", err)
	}

	if !reflect.DeepEqual(
		copyState(actual),
		copyState(expected),
	) {
		t.Fatalf(
			"recovered state mismatch:\nexpected=%+v\nactual=%+v",
			expected,
			actual,
		)
	}
}

// Test 1: A rename failure must preserve the original
// WAL file and its recoverable Raft state.
func TestPhysicalCompactionRenameFailurePreservesWAL(
	t *testing.T,
) {
	path, w, expected := preparePhysicalCompactionFailureWAL(t)

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	injectedErr := errors.New("injected rename failure")

	ops := defaultPhysicalCompactionOps()

	renameCalled := false

	ops.rename = func(oldPath, newPath string) error {
		renameCalled = true
		return injectedErr
	}

	err = w.rewriteCheckpointWithOps(path, ops)

	if !errors.Is(err, injectedErr) {
		t.Fatalf(
			"expected injected rename failure, got %v",
			err,
		)
	}

	if !renameCalled {
		t.Fatal("rename hook was not called")
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(before, after) {
		t.Fatal("original WAL changed after rename failure")
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	recoverPhysicalCompactionFailureWAL(
		t,
		path,
		expected,
	)
}

// Test 2: If directory synchronization fails after
// rename, the WAL must become unusable until recovery.
func TestPhysicalCompactionDirectorySyncFailureStopsWrites(
	t *testing.T,
) {
	path, w, expected := preparePhysicalCompactionFailureWAL(t)

	injectedErr := errors.New(
		"injected directory sync failure",
	)

	ops := defaultPhysicalCompactionOps()

	syncCalled := false

	ops.syncDir = func(dir string) error {
		syncCalled = true
		return injectedErr
	}

	err := w.rewriteCheckpointWithOps(path, ops)

	if !errors.Is(err, injectedErr) {
		t.Fatalf(
			"expected injected sync failure, got %v",
			err,
		)
	}

	if !syncCalled {
		t.Fatal("directory sync hook was not called")
	}

	if !w.unusable {
		t.Fatal(
			"WAL should be unusable after directory sync failure",
		)
	}

	err = w.AppendEntries([]raft.Entry{
		{Index: 5, Term: 3},
	})

	if err == nil {
		t.Fatal(
			"expected append to fail after directory sync failure",
		)
	}

	// The injected failure represents a reported
	// synchronization error, not a real power loss.
	// Recovery is checked from the on-disk checkpoint.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	recoverPhysicalCompactionFailureWAL(
		t,
		path,
		expected,
	)
}

// Test 3: A failed rename must not leave temporary
// WAL files behind.
func TestPhysicalCompactionRenameFailureCleansTempFile(
	t *testing.T,
) {
	path, w, _ := preparePhysicalCompactionFailureWAL(t)
	defer w.Close()

	ops := defaultPhysicalCompactionOps()

	ops.rename = func(oldPath, newPath string) error {
		return errors.New("injected rename failure")
	}

	err := w.rewriteCheckpointWithOps(path, ops)
	if err == nil {
		t.Fatal("expected rename failure")
	}

	matches, err := filepath.Glob(
		filepath.Join(filepath.Dir(path), ".wal-rewrite-*"),
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(matches) != 0 {
		t.Fatalf(
			"temporary WAL files were not removed: %v",
			matches,
		)
	}
}

// Test 4: Missing filesystem hooks must be rejected
// without modifying the WAL.
func TestPhysicalCompactionRejectsMissingHooks(
	t *testing.T,
) {
	path, w, _ := preparePhysicalCompactionFailureWAL(t)
	defer w.Close()

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	err = w.rewriteCheckpointWithOps(
		path,
		physicalCompactionOps{},
	)

	if !errors.Is(err, ErrInvalid) {
		t.Fatalf(
			"expected ErrInvalid, got %v",
			err,
		)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(before, after) {
		t.Fatal("WAL changed despite missing hooks")
	}
}

// Test 5: A successful rewrite using injected hooks
// must preserve normal WAL behavior.
func TestPhysicalCompactionWithHooksSucceeds(
	t *testing.T,
) {
	path, w, expected := preparePhysicalCompactionFailureWAL(t)

	ops := defaultPhysicalCompactionOps()

	renameCalled := false
	syncCalled := false

	realRename := ops.rename
	realSyncDir := ops.syncDir

	ops.rename = func(oldPath, newPath string) error {
		renameCalled = true
		return realRename(oldPath, newPath)
	}

	ops.syncDir = func(dir string) error {
		syncCalled = true
		return realSyncDir(dir)
	}

	if err := w.rewriteCheckpointWithOps(path, ops); err != nil {
		t.Fatal(err)
	}

	if !renameCalled || !syncCalled {
		t.Fatalf(
			"expected both hooks: rename=%v sync=%v",
			renameCalled,
			syncCalled,
		)
	}

	if w.unusable {
		t.Fatal("WAL should remain usable after success")
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	recoverPhysicalCompactionFailureWAL(
		t,
		path,
		expected,
	)
}
