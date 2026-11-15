package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSegmentDiscoveryIgnoresCheckpointFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	dir := segmentDirectory(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	for _, number := range []uint64{1, 2, 3} {
		if err := os.WriteFile(
			segmentPath(path, number),
			nil,
			0644,
		); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(
		checkpointPath(path, 4),
		[]byte("unpublished checkpoint"),
		0644,
	); err != nil {
		t.Fatal(err)
	}

	numbers, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint64{1, 2, 3}

	if !reflect.DeepEqual(numbers, want) {
		t.Fatalf(
			"segments = %v, want %v",
			numbers,
			want,
		)
	}
}
