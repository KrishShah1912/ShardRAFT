package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestAcceptsExplicitCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path),
		0755,
	); err != nil {
		t.Fatal(err)
	}

	want := segmentedManifest{
		Version:           segmentedManifestVersion,
		FirstSegment:      5,
		CheckpointSegment: 5,
	}

	if err := writeSegmentedManifest(path, want); err != nil {
		t.Fatal(err)
	}

	got, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}

	if !found {
		t.Fatal("manifest missing")
	}

	if got != want {
		t.Fatalf(
			"manifest = %+v, want %+v",
			got,
			want,
		)
	}
}

func TestManifestRejectsCheckpointMismatch(t *testing.T) {
	manifest := segmentedManifest{
		Version:           segmentedManifestVersion,
		FirstSegment:      5,
		CheckpointSegment: 6,
	}

	if err := manifest.validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
}
