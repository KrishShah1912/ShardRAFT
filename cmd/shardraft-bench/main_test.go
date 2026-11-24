package main

import (
	"testing"
	"time"
)

func TestValidateConfig(t *testing.T) {
	valid := config{
		target:      "http://127.0.0.1:8081",
		requests:    100,
		concurrency: 4,
		readRatio:   0.8,
		valueSize:   128,
		timeout:     time.Second,
	}

	if err := validateConfig(valid); err != nil {
		t.Fatalf(
			"valid config rejected: %v",
			err,
		)
	}

	tests := []struct {
		name string
		cfg  config
	}{
		{
			name: "zero requests",
			cfg: func() config {
				value := valid
				value.requests = 0
				return value
			}(),
		},
		{
			name: "zero concurrency",
			cfg: func() config {
				value := valid
				value.concurrency = 0
				return value
			}(),
		},
		{
			name: "negative read ratio",
			cfg: func() config {
				value := valid
				value.readRatio = -0.1
				return value
			}(),
		},
		{
			name: "read ratio over one",
			cfg: func() config {
				value := valid
				value.readRatio = 1.1
				return value
			}(),
		},
		{
			name: "negative value size",
			cfg: func() config {
				value := valid
				value.valueSize = -1
				return value
			}(),
		},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				if err := validateConfig(test.cfg); err == nil {
					t.Fatal(
						"invalid configuration accepted",
					)
				}
			},
		)
	}
}

func TestPercentile(t *testing.T) {
	values := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		4 * time.Millisecond,
		5 * time.Millisecond,
	}

	tests := []struct {
		name string
		p    float64
		want time.Duration
	}{
		{
			name: "p50",
			p:    0.50,
			want: 3 * time.Millisecond,
		},
		{
			name: "p95",
			p:    0.95,
			want: 5 * time.Millisecond,
		},
		{
			name: "p99",
			p:    0.99,
			want: 5 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				got := percentile(
					values,
					test.p,
				)

				if got != test.want {
					t.Fatalf(
						"got %s, want %s",
						got,
						test.want,
					)
				}
			},
		)
	}
}

func TestOperationIsReadBoundaries(t *testing.T) {
	for index := 0; index < 100; index++ {
		if operationIsRead(index, 0) {
			t.Fatal(
				"ratio 0 produced a read",
			)
		}

		if !operationIsRead(index, 1) {
			t.Fatal(
				"ratio 1 produced a write",
			)
		}
	}
}

func TestOperationReadDistribution(t *testing.T) {
	const operations = 10000

	reads := 0

	for index := 0; index < operations; index++ {
		if operationIsRead(index, 0.8) {
			reads++
		}
	}

	ratio := float64(reads) /
		float64(operations)

	if ratio < 0.79 ||
		ratio > 0.81 {
		t.Fatalf(
			"read ratio %.4f outside expected range",
			ratio,
		)
	}
}

func TestCandidateURLsIncludesCluster(t *testing.T) {
	urls := candidateURLs(
		"http://127.0.0.1:8082",
	)

	if len(urls) != 3 {
		t.Fatalf(
			"got %d URLs, want 3",
			len(urls),
		)
	}

	seen := make(map[string]bool)

	for _, url := range urls {
		seen[url] = true
	}

	expected := []string{
		"http://127.0.0.1:8081",
		"http://127.0.0.1:8082",
		"http://127.0.0.1:8083",
	}

	for _, url := range expected {
		if !seen[url] {
			t.Fatalf(
				"missing URL %s",
				url,
			)
		}
	}
}

func TestNormalizeURL(t *testing.T) {
	got := normalizeURL(
		"http://127.0.0.1:8081///",
	)

	want :=
		"http://127.0.0.1:8081"

	if got != want {
		t.Fatalf(
			"got %q, want %q",
			got,
			want,
		)
	}
}
