package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func setupManifestFailureTest(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "node.wal")

	if err := os.MkdirAll(
		segmentDirectory(path),
		0755,
	); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestSegmentedManifestRenameFailure(t *testing.T) {
	path := setupManifestFailureTest(t)

	injected := errors.New("injected manifest rename failure")
	ops := defaultPhysicalCompactionOps()

	ops.rename = func(oldPath, newPath string) error {
		return injected
	}

	err := writeSegmentedManifestWithOps(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 2,
		},
		ops,
	)

	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	_, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("manifest published despite rename failure")
	}

	matches, err := filepath.Glob(
		filepath.Join(segmentDirectory(path), ".manifest-*"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary manifest files remain: %v", matches)
	}
}

func TestSegmentedManifestSyncFailure(t *testing.T) {
	path := setupManifestFailureTest(t)

	injected := errors.New("injected manifest sync failure")
	ops := defaultPhysicalCompactionOps()

	ops.syncDir = func(string) error {
		return injected
	}

	err := writeSegmentedManifestWithOps(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 2,
		},
		ops,
	)

	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	// Rename succeeded before the injected sync error.
	// The manifest is visible, but crash durability is
	// not guaranteed.
	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || manifest.FirstSegment != 2 {
		t.Fatal("expected visible manifest after rename")
	}
}

func TestSegmentedManifestRejectsMissingOps(t *testing.T) {
	path := setupManifestFailureTest(t)

	err := writeSegmentedManifestWithOps(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 1,
		},
		physicalCompactionOps{},
	)

	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}

	if _, err := os.Stat(manifestPath(path)); !os.IsNotExist(err) {
		t.Fatalf("unexpected manifest: %v", err)
	}
}

func TestSegmentedManifestWithOpsSucceeds(t *testing.T) {
	path := setupManifestFailureTest(t)

	ops := defaultPhysicalCompactionOps()

	renameCalled := false
	syncCalled := false

	realRename := ops.rename
	realSync := ops.syncDir

	ops.rename = func(oldPath, newPath string) error {
		renameCalled = true
		return realRename(oldPath, newPath)
	}

	ops.syncDir = func(dir string) error {
		syncCalled = true
		return realSync(dir)
	}

	err := writeSegmentedManifestWithOps(
		path,
		segmentedManifest{
			Version:      segmentedManifestVersion,
			FirstSegment: 3,
		},
		ops,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !renameCalled || !syncCalled {
		t.Fatal("expected rename and sync hooks")
	}

	manifest, found, err := readSegmentedManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || manifest.FirstSegment != 3 {
		t.Fatal("published manifest mismatch")
	}
}
