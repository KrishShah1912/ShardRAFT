package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestManifestRecoveryRejectsEmptyCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := writeSegmentedManifest(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 1,
		},
	); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := reopened.Recover(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestManifestRecoveryRejectsNonCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("command"),
	}}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := writeSegmentedManifest(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 1,
		},
	); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := reopened.Recover(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestManifestRecoveryRejectsMissingSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(segmentPath(path, 1)); err != nil {
		t.Fatal(err)
	}

	if err := writeSegmentedManifest(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 1,
		},
	); err != nil {
		t.Fatal(err)
	}

	_, err = OpenSegmented(path, SegmentOptions{})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}
