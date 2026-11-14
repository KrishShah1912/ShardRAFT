package snapshot

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSnapshotSaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")

	original := New(
		5,
		2,
		map[string]string{
			"name": "ShardRAFT",
			"x":    "10",
		},
	)

	if err := Save(path, original); err != nil {
		t.Fatal(err)
	}

	restored, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if restored.AppliedIndex != 5 {
		t.Fatalf(
			"expected applied index 5, got %d",
			restored.AppliedIndex,
		)
	}

	if restored.AppliedTerm != 2 {
		t.Fatalf(
			"expected applied term 2, got %d",
			restored.AppliedTerm,
		)
	}

	if !reflect.DeepEqual(original, restored) {
		t.Fatalf(
			"snapshot mismatch: %+v != %+v",
			original,
			restored,
		)
	}
}

func TestSnapshotDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")

	s := New(
		1,
		1,
		map[string]string{"x": "10"},
	)

	if err := Save(path, s); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Modify snapshot content without updating checksum.
	modified := []byte(
		string(raw[:len(raw)-1]) + " ",
	)

	if err := os.WriteFile(path, modified, 0600); err != nil {
		t.Fatal(err)
	}

	_, err = Load(path)

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf(
			"expected ErrCorrupt, got %v",
			err,
		)
	}
}

func TestSnapshotRejectsInvalidMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")

	s := New(
		3,
		0,
		map[string]string{"x": "10"},
	)

	err := Save(path, s)

	if !errors.Is(err, ErrInvalid) {
		t.Fatalf(
			"expected ErrInvalid, got %v",
			err,
		)
	}
}

func TestSnapshotCopiesData(t *testing.T) {
	data := map[string]string{
		"x": "10",
	}

	s := New(1, 1, data)

	data["x"] = "999"

	if s.Data["x"] != "10" {
		t.Fatal("snapshot shares mutable input data")
	}
}

func TestSnapshotReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")

	first := New(
		1,
		1,
		map[string]string{"x": "10"},
	)

	second := New(
		2,
		1,
		map[string]string{"x": "20"},
	)

	if err := Save(path, first); err != nil {
		t.Fatal(err)
	}

	if err := Save(path, second); err != nil {
		t.Fatal(err)
	}

	restored, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if restored.AppliedIndex != 2 {
		t.Fatal("snapshot replacement did not update index")
	}

	if restored.Data["x"] != "20" {
		t.Fatal("snapshot replacement did not update data")
	}
}

func TestSnapshotMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")

	_, err := Load(path)

	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(
			"expected missing file error, got %v",
			err,
		)
	}
}
