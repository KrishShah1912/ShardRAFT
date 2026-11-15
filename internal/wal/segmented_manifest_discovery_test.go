package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestManifestAllowsRetiredPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path), 0755,
	); err != nil {
		t.Fatal(err)
	}

	for _, number := range []uint64{3, 4, 5} {
		if err := os.WriteFile(
			segmentPath(path, number),
			nil,
			0644,
		); err != nil {
			t.Fatal(err)
		}
	}

	if err := writeSegmentedManifest(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 3,
		},
	); err != nil {
		t.Fatal(err)
	}

	numbers, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(numbers, []uint64{3, 4, 5}) {
		t.Fatalf("segments = %v", numbers)
	}
}

func TestManifestRejectsMissingCheckpointSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path), 0755,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		segmentPath(path, 4),
		nil,
		0644,
	); err != nil {
		t.Fatal(err)
	}

	if err := writeSegmentedManifest(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 3,
		},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := listSegments(path); err == nil {
		t.Fatal("expected missing checkpoint segment error")
	}
}

func TestManifestIgnoresTemporaryFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path), 0755,
	); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{
		".manifest-interrupted",
		".checkpoint-interrupted",
	} {
		if err := os.WriteFile(
			filepath.Join(segmentDirectory(path), name),
			nil,
			0644,
		); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(
		segmentPath(path, 1),
		nil,
		0644,
	); err != nil {
		t.Fatal(err)
	}

	numbers, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(numbers, []uint64{1}) {
		t.Fatalf("segments = %v", numbers)
	}
}
