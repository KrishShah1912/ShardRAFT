package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSegmentedOpenCreatesFirstSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if w.number != 1 {
		t.Fatalf("segment number = %d, want 1", w.number)
	}

	if _, err := os.Stat(segmentPath(path, 1)); err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
	}}); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentedOpenExistingRequiresRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	w, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendEntries([]raft.Entry{{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
	}}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSegmented(path, SegmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if err := reopened.AppendEntries([]raft.Entry{{
		Index: 2,
		Term:  1,
		Type:  raft.EntryNormal,
	}}); !errors.Is(err, ErrNotRecovered) {
		t.Fatalf("expected ErrNotRecovered, got %v", err)
	}

	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentedOpenRejectsMissingManifestSegment(t *testing.T) {
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

	if _, err := OpenSegmented(path, SegmentOptions{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}

	if _, err := os.Stat(segmentPath(path, 1)); !os.IsNotExist(err) {
		t.Fatalf("missing segment was recreated: %v", err)
	}
}
