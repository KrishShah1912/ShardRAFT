package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentedManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path), 0755,
	); err != nil {
		t.Fatal(err)
	}

	expected := segmentedManifest{
		Version:      segmentedManifestVersion,
		FirstSegment: 7,
	}

	if err := writeSegmentedManifest(path, expected); err != nil {
		t.Fatal(err)
	}

	actual, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}

	if !found {
		t.Fatal("expected manifest")
	}

	if actual != expected {
		t.Fatalf(
			"manifest = %+v, want %+v",
			actual, expected,
		)
	}
}

func TestSegmentedManifestMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	_, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}

	if found {
		t.Fatal("unexpected manifest")
	}
}

func TestSegmentedManifestRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path), 0755,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		manifestPath(path),
		[]byte("invalid-json"),
		0644,
	); err != nil {
		t.Fatal(err)
	}

	if _, _, err := readSegmentedManifest(path); err == nil {
		t.Fatal("expected corrupted manifest error")
	}
}
