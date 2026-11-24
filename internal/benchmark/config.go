package benchmark

import (
	"errors"
	"strings"
	"time"
)

const (
	MinRequests    = 100
	MaxRequests    = 50000
	MinConcurrency = 1
	MaxConcurrency = 64
	MaxValueSize   = 4096
)

func DefaultConfig() Config {
	return Config{
		Target:      "http://127.0.0.1:8081",
		Requests:    1000,
		Concurrency: 8,
		ReadRatio:   0.80,
		ValueSize:   128,
		Timeout:     5 * time.Second,
	}
}

func ValidateConfig(cfg Config) error {
	if cfg.Requests < MinRequests ||
		cfg.Requests > MaxRequests {
		return errors.New(
			"requests must be between 100 and 50000",
		)
	}

	if cfg.Concurrency < MinConcurrency ||
		cfg.Concurrency > MaxConcurrency {
		return errors.New(
			"concurrency must be between 1 and 64",
		)
	}

	if cfg.ReadRatio < 0 ||
		cfg.ReadRatio > 1 {
		return errors.New(
			"read ratio must be between 0 and 1",
		)
	}

	if cfg.ValueSize < 0 ||
		cfg.ValueSize > MaxValueSize {
		return errors.New(
			"value size must be between 0 and 4096",
		)
	}

	if strings.TrimSpace(cfg.Target) == "" {
		return errors.New(
			"target cannot be empty",
		)
	}

	return nil
}
