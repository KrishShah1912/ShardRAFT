package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCheckpointFilename(t *testing.T) {
	got := checkpointFilename(9)
	want := "00000000000000000009.chk"

	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestListCheckpointFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	dir := segmentDirectory(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	for _, number := range []uint64{7, 3, 12} {
		if err := os.WriteFile(
			checkpointPath(path, number),
			[]byte("test"),
			0644,
		); err != nil {
			t.Fatal(err)
		}
	}

	got, err := listCheckpointFiles(path)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint64{3, 7, 12}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCheckpointFileDoesNotImplyPublication(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "node.wal")
	dir := segmentDirectory(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		checkpointPath(path, 5),
		[]byte("unpublished"),
		0644,
	); err != nil {
		t.Fatal(err)
	}

	_, published, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}

	if published {
		t.Fatal(
			"checkpoint file was treated as published",
		)
	}
}
