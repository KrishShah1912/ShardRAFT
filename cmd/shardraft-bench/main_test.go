package main

import (
	"flag"
	"os"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/benchmark"
)

func TestParseFlagsDefaults(t *testing.T) {
	originalArgs := os.Args
	originalFlags := flag.CommandLine

	defer func() {
		os.Args = originalArgs
		flag.CommandLine = originalFlags
	}()

	flag.CommandLine = flag.NewFlagSet(
		"test",
		flag.ContinueOnError,
	)

	os.Args = []string{
		"shardraft-bench",
	}

	cfg := parseFlags()
	want := benchmark.DefaultConfig()

	if cfg.Target != want.Target {
		t.Fatalf(
			"target = %q, want %q",
			cfg.Target,
			want.Target,
		)
	}

	if cfg.Requests != want.Requests {
		t.Fatalf(
			"requests = %d, want %d",
			cfg.Requests,
			want.Requests,
		)
	}

	if cfg.Concurrency != want.Concurrency {
		t.Fatalf(
			"concurrency = %d, want %d",
			cfg.Concurrency,
			want.Concurrency,
		)
	}

	if cfg.ReadRatio != want.ReadRatio {
		t.Fatalf(
			"read ratio = %f, want %f",
			cfg.ReadRatio,
			want.ReadRatio,
		)
	}

	if cfg.ValueSize != want.ValueSize {
		t.Fatalf(
			"value size = %d, want %d",
			cfg.ValueSize,
			want.ValueSize,
		)
	}
}

func TestParseFlagsOverrides(t *testing.T) {
	originalArgs := os.Args
	originalFlags := flag.CommandLine

	defer func() {
		os.Args = originalArgs
		flag.CommandLine = originalFlags
	}()

	flag.CommandLine = flag.NewFlagSet(
		"test",
		flag.ContinueOnError,
	)

	os.Args = []string{
		"shardraft-bench",
		"-target",
		"http://127.0.0.1:8082",
		"-requests",
		"2000",
		"-concurrency",
		"16",
		"-read-ratio",
		"0.65",
		"-value-size",
		"512",
	}

	cfg := parseFlags()

	if cfg.Target != "http://127.0.0.1:8082" {
		t.Fatalf(
			"unexpected target %q",
			cfg.Target,
		)
	}

	if cfg.Requests != 2000 {
		t.Fatalf(
			"requests = %d, want 2000",
			cfg.Requests,
		)
	}

	if cfg.Concurrency != 16 {
		t.Fatalf(
			"concurrency = %d, want 16",
			cfg.Concurrency,
		)
	}

	if cfg.ReadRatio != 0.65 {
		t.Fatalf(
			"read ratio = %f, want 0.65",
			cfg.ReadRatio,
		)
	}

	if cfg.ValueSize != 512 {
		t.Fatalf(
			"value size = %d, want 512",
			cfg.ValueSize,
		)
	}
}
