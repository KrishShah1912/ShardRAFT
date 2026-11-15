package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointReferenceRejectsMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	manifest := segmentedManifest{
		Version:           segmentedManifestVersion,
		FirstSegment:      5,
		CheckpointSegment: 5,
	}

	err := validateCheckpointReference(path, manifest)

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestCheckpointReferenceRejectsEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path),
		0755,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		checkpointPath(path, 5),
		nil,
		0644,
	); err != nil {
		t.Fatal(err)
	}

	manifest := segmentedManifest{
		Version:           segmentedManifestVersion,
		FirstSegment:      5,
		CheckpointSegment: 5,
	}

	err := validateCheckpointReference(path, manifest)

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestCheckpointReferenceAcceptsExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path),
		0755,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		checkpointPath(path, 5),
		[]byte("checkpoint"),
		0644,
	); err != nil {
		t.Fatal(err)
	}

	manifest := segmentedManifest{
		Version:           segmentedManifestVersion,
		FirstSegment:      5,
		CheckpointSegment: 5,
	}

	if err := validateCheckpointReference(
		path,
		manifest,
	); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointReferenceAllowsLegacyManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	manifest := segmentedManifest{
		Version:      segmentedManifestVersion,
		FirstSegment: 5,
	}

	if err := validateCheckpointReference(
		path,
		manifest,
	); err != nil {
		t.Fatal(err)
	}
}
