package wal

import (
	"errors"
	"testing"
)

func TestSegmentedManifestAcceptsGenerationBoundary(t *testing.T) {
	tests := []struct {
		name     string
		manifest segmentedManifest
	}{
		{
			name: "legacy manifest",
			manifest: segmentedManifest{
				Version:      segmentedManifestVersion,
				FirstSegment: 2,
			},
		},
		{
			name: "single segment generation",
			manifest: segmentedManifest{
				Version:      segmentedManifestVersion,
				FirstSegment: 3,
				LastSegment:  3,
			},
		},
		{
			name: "multiple segments",
			manifest: segmentedManifest{
				Version:      segmentedManifestVersion,
				FirstSegment: 3,
				LastSegment:  7,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.manifest.validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSegmentedManifestRejectsInvalidBoundary(t *testing.T) {
	manifest := segmentedManifest{
		Version:      segmentedManifestVersion,
		FirstSegment: 8,
		LastSegment:  7,
	}

	if err := manifest.validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
}

func TestSegmentedManifestBoundaryRoundTrip(t *testing.T) {
	path := setupManifestFailureTest(t)

	want := segmentedManifest{
		Version:      segmentedManifestVersion,
		FirstSegment: 4,
		LastSegment:  9,
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
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
