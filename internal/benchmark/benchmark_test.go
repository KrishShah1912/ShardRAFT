package benchmark

import (
	"testing"
	"time"
)

func TestValidateConfig(t *testing.T) {
	cfg := DefaultConfig()

	if err := ValidateConfig(cfg); err != nil {
		t.Fatalf(
			"default config invalid: %v",
			err,
		)
	}
}

func TestValidateConfigRejectsLargeRun(
	t *testing.T,
) {
	cfg := DefaultConfig()
	cfg.Requests = MaxRequests + 1

	if err := ValidateConfig(cfg); err == nil {
		t.Fatal(
			"expected oversized run to fail",
		)
	}
}

func TestOperationIsReadBoundaries(
	t *testing.T,
) {
	if operationIsRead(0, 0) {
		t.Fatal(
			"zero ratio produced read",
		)
	}

	if !operationIsRead(0, 1) {
		t.Fatal(
			"one ratio did not produce read",
		)
	}
}

func TestLatencyStats(t *testing.T) {
	values := []int64{
		1,
		2,
		3,
		4,
		5,
	}

	durations :=
		make(
			[]time.Duration,
			len(values),
		)

	for i, value := range values {
		durations[i] =
			time.Duration(value) *
				time.Millisecond
	}

	stats :=
		latencyStats(durations)

	if stats.P50MS != 3 {
		t.Fatalf(
			"p50 = %f, want 3",
			stats.P50MS,
		)
	}

	if stats.MaxMS != 5 {
		t.Fatalf(
			"max = %f, want 5",
			stats.MaxMS,
		)
	}
}

func TestOperationReadDistribution(
	t *testing.T,
) {
	const operations = 10000

	reads := 0

	for index := 0; index < operations; index++ {

		if operationIsRead(
			index,
			0.80,
		) {
			reads++
		}
	}

	ratio :=
		float64(reads) /
			float64(operations)

	if ratio < 0.79 ||
		ratio > 0.81 {

		t.Fatalf(
			"read ratio %.4f outside expected range",
			ratio,
		)
	}
}

func TestValidateConfigRejectsInvalidValues(
	t *testing.T,
) {
	valid := DefaultConfig()

	tests := []struct {
		name string
		edit func(*Config)
	}{
		{
			name: "too few requests",
			edit: func(cfg *Config) {
				cfg.Requests =
					MinRequests - 1
			},
		},
		{
			name: "too many requests",
			edit: func(cfg *Config) {
				cfg.Requests =
					MaxRequests + 1
			},
		},
		{
			name: "zero concurrency",
			edit: func(cfg *Config) {
				cfg.Concurrency = 0
			},
		},
		{
			name: "too much concurrency",
			edit: func(cfg *Config) {
				cfg.Concurrency =
					MaxConcurrency + 1
			},
		},
		{
			name: "negative read ratio",
			edit: func(cfg *Config) {
				cfg.ReadRatio = -0.1
			},
		},
		{
			name: "read ratio over one",
			edit: func(cfg *Config) {
				cfg.ReadRatio = 1.1
			},
		},
		{
			name: "negative value size",
			edit: func(cfg *Config) {
				cfg.ValueSize = -1
			},
		},
		{
			name: "oversized value",
			edit: func(cfg *Config) {
				cfg.ValueSize =
					MaxValueSize + 1
			},
		},
		{
			name: "empty target",
			edit: func(cfg *Config) {
				cfg.Target = ""
			},
		},
	}

	for _, test := range tests {

		t.Run(
			test.name,
			func(t *testing.T) {
				cfg := valid

				test.edit(&cfg)

				if err :=
					ValidateConfig(
						cfg,
					); err == nil {

					t.Fatal(
						"invalid configuration accepted",
					)
				}
			},
		)
	}
}
