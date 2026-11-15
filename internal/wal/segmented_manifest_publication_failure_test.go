package wal

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExplicitCheckpointManifestRenameFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	options := SegmentOptions{MaxSegmentSize: 256}

	w := createCrashRecoveryWAL(t, path, options)
	expected := copyState(w.state)

	injected := errors.New("injected manifest rename failure")

	ops := defaultPhysicalCompactionOps()
	ops.rename = func(_, _ string) error {
		return injected
	}

	w.manifestPublishOps = &ops

	err := w.PublishCheckpoint()
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	if !w.unusable {
		t.Fatal("WAL remains usable after publication failure")
	}

	if err := w.Sync(); !errors.Is(err, ErrUnusable) {
		t.Fatalf("expected ErrUnusable, got %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// The failed rename must not publish a new manifest.
	_, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("failed rename unexpectedly published manifest")
	}

	// An unpublished .chk may remain, but recovery
	// must use the previous durable WAL generation.
	reopened, err := OpenSegmented(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	actual, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		copyState(actual),
		copyState(expected),
	) {
		t.Fatal("state changed after failed publication")
	}

	// Verify that the staged checkpoint is not authoritative.
	checkpoints, err := listCheckpointFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) == 0 {
		t.Fatal("expected an unpublished checkpoint candidate")
	}

	if _, err := os.Stat(
		checkpointPath(path, checkpoints[0]),
	); err != nil {
		t.Fatal(err)
	}
}
