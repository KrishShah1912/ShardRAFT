package wal

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRetryPublicationAfterOrphanCheckpoint(t *testing.T) {
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

	if err := w.PublishCheckpoint(); !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

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
		t.Fatal("previous durable state was not recovered")
	}

	// Retry checkpoint publication after restart.
	if err := reopened.PublishCheckpoint(); err != nil {
		t.Fatalf("checkpoint retry failed: %v", err)
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || manifest.CheckpointSegment == 0 {
		t.Fatal("retry did not publish explicit checkpoint")
	}

	if err := reopened.VerifyPublishedCheckpoint(); err != nil {
		t.Fatal(err)
	}
}
