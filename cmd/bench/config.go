package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	Targets      []string
	Requests     int
	Concurrency  int
	Keys         int
	ReadRatio    float64
	ValueSize    int
	Distribution string
	ZipfS        float64
	Seed         int64
	Timeout      time.Duration
	OutputDir    string
}

func parseConfig(args []string, stderr io.Writer) (Config, error) {
	var cfg Config

	flags := flag.NewFlagSet("bench", flag.ContinueOnError)
	flags.SetOutput(stderr)

	targets := flags.String(
		"targets",
		"http://127.0.0.1:18091,http://127.0.0.1:18092",
		"comma-separated gateway URLs",
	)

	flags.IntVar(&cfg.Requests, "requests", 1000, "operation count")
	flags.IntVar(&cfg.Concurrency, "concurrency", 8, "worker count")
	flags.IntVar(&cfg.Keys, "keys", 100, "key space size")
	flags.Float64Var(&cfg.ReadRatio, "read-ratio", 0.5, "read fraction")
	flags.IntVar(&cfg.ValueSize, "value-size", 128, "value size in bytes")

	flags.StringVar(
		&cfg.Distribution,
		"distribution",
		"uniform",
		"uniform or zipfian",
	)

	flags.Float64Var(&cfg.ZipfS, "zipf-s", 1.2, "Zipf exponent")
	flags.Int64Var(&cfg.Seed, "seed", 42, "workload seed")
	flags.DurationVar(&cfg.Timeout, "timeout", 15*time.Second, "request timeout")
	flags.StringVar(&cfg.OutputDir, "output", "results", "results directory")

	if err := flags.Parse(args); err != nil {
		return Config{}, err
	}

	if flags.NArg() != 0 {
		return Config{}, fmt.Errorf("unexpected positional arguments")
	}

	for _, target := range strings.Split(*targets, ",") {
		cfg.Targets = append(
			cfg.Targets,
			strings.TrimRight(strings.TrimSpace(target), "/"),
		)
	}

	return cfg, cfg.Validate()
}

func (cfg Config) Validate() error {
	if len(cfg.Targets) == 0 {
		return fmt.Errorf("at least one target is required")
	}

	for _, target := range cfg.Targets {
		u, err := url.Parse(target)

		if err != nil || u.Host == "" ||
			(u.Scheme != "http" && u.Scheme != "https") ||
			u.User != nil || u.RawQuery != "" ||
			u.Fragment != "" || u.Path != "" {
			return fmt.Errorf("invalid gateway URL: %q", target)
		}
	}

	if cfg.Requests < 1 || cfg.Requests > 1000000 {
		return fmt.Errorf("requests must be between 1 and 1000000")
	}

	if cfg.Concurrency < 1 || cfg.Concurrency > cfg.Requests ||
		cfg.Concurrency > 1024 {
		return fmt.Errorf("invalid concurrency")
	}

	if cfg.Keys < 1 || cfg.Keys > 1000000 {
		return fmt.Errorf("invalid key count")
	}

	if math.IsNaN(cfg.ReadRatio) ||
		cfg.ReadRatio < 0 || cfg.ReadRatio > 1 {
		return fmt.Errorf("read ratio must be between 0 and 1")
	}

	if cfg.ValueSize < 1 || cfg.ValueSize > 512*1024 {
		return fmt.Errorf("value size must be between 1 and 524288")
	}

	if cfg.Distribution != "uniform" &&
		cfg.Distribution != "zipfian" {
		return fmt.Errorf("unsupported distribution")
	}

	if math.IsNaN(cfg.ZipfS) || math.IsInf(cfg.ZipfS, 0) ||
		cfg.ZipfS <= 1 {
		return fmt.Errorf("Zipf exponent must exceed 1")
	}

	if cfg.Timeout <= 0 || cfg.OutputDir == "" {
		return fmt.Errorf("invalid timeout or output directory")
	}

	// Bound retained history size for local experiments.
	if int64(cfg.Requests)*int64(cfg.ValueSize) > 256*1024*1024 {
		return fmt.Errorf("workload exceeds history memory budget")
	}

	return nil
}
