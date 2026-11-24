package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestBenchmarkHistoryRoundTrip(
	t *testing.T,
) {
	path :=
		filepath.Join(
			t.TempDir(),
			"history.json",
		)

	runs := []benchmarkJob{
		{
			ID: "run-1",

			State: benchmarkPassed,

			StartedAt: time.Now(),

			Config: benchmarkRequest{
				Requests:    1000,
				Concurrency: 8,
				ReadRatio:   0.8,
				ValueSize:   128,
			},
		},
	}

	if err :=
		saveBenchmarkHistory(
			path,
			runs,
		); err != nil {

		t.Fatalf(
			"save history: %v",
			err,
		)
	}

	loaded, err :=
		loadBenchmarkHistory(
			path,
		)

	if err != nil {
		t.Fatalf(
			"load history: %v",
			err,
		)
	}

	if len(loaded) != 1 {
		t.Fatalf(
			"loaded %d runs, want 1",
			len(loaded),
		)
	}

	if loaded[0].ID !=
		"run-1" {

		t.Fatalf(
			"loaded ID %q",
			loaded[0].ID,
		)
	}
}

func TestMissingBenchmarkHistory(
	t *testing.T,
) {
	path :=
		filepath.Join(
			t.TempDir(),
			"missing.json",
		)

	runs, err :=
		loadBenchmarkHistory(
			path,
		)

	if err != nil {
		t.Fatalf(
			"load missing history: %v",
			err,
		)
	}

	if len(runs) != 0 {
		t.Fatalf(
			"got %d runs, want 0",
			len(runs),
		)
	}
}

func TestBenchmarkHistoryLimit(
	t *testing.T,
) {
	path :=
		filepath.Join(
			t.TempDir(),
			"history.json",
		)

	runs :=
		make(
			[]benchmarkJob,
			benchmarkHistoryLimit+10,
		)

	for index := range runs {

		runs[index] =
			benchmarkJob{
				ID: string(
					rune(
						'A' +
							index%26,
					),
				),
				State: benchmarkPassed,
			}
	}

	if err :=
		saveBenchmarkHistory(
			path,
			runs,
		); err != nil {

		t.Fatalf(
			"save history: %v",
			err,
		)
	}

	loaded, err :=
		loadBenchmarkHistory(
			path,
		)

	if err != nil {
		t.Fatalf(
			"load history: %v",
			err,
		)
	}

	if len(loaded) !=
		benchmarkHistoryLimit {

		t.Fatalf(
			"loaded %d runs, want %d",
			len(loaded),
			benchmarkHistoryLimit,
		)
	}
}
