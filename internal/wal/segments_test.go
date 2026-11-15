package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSegmentNaming(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	got := segmentPath(path, 7)
	want := filepath.Join(
		path+".segments",
		"00000000000000000007.seg",
	)

	if got != want {
		t.Fatalf("segment path = %q, want %q", got, want)
	}
}

func TestListSegmentsSorted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	dir := segmentDirectory(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	for _, number := range []uint64{3, 1, 2} {
		if err := os.WriteFile(
			segmentPath(path, number), nil, 0644,
		); err != nil {
			t.Fatal(err)
		}
	}

	got, err := listSegments(path)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint64{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("segments = %v, want %v", got, want)
	}
}

func TestListSegmentsRejectsGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	dir := segmentDirectory(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	for _, number := range []uint64{1, 3} {
		if err := os.WriteFile(
			segmentPath(path, number), nil, 0644,
		); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := listSegments(path); err == nil {
		t.Fatal("expected segment numbering gap error")
	}
}

func TestListSegmentsRejectsMalformedName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	dir := segmentDirectory(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(dir, "bad.seg"), nil, 0644,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := listSegments(path); err == nil {
		t.Fatal("expected malformed segment name error")
	}
}
