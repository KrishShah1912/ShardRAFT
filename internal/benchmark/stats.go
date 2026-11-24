package benchmark

import (
	"math"
	"sort"
	"time"
)

func latencyStats(
	values []time.Duration,
) LatencyStats {
	if len(values) == 0 {
		return LatencyStats{}
	}

	sorted := append(
		[]time.Duration(nil),
		values...,
	)

	sort.Slice(
		sorted,
		func(i, j int) bool {
			return sorted[i] < sorted[j]
		},
	)

	var total time.Duration

	for _, value := range sorted {
		total += value
	}

	average :=
		total / time.Duration(len(sorted))

	return LatencyStats{
		AverageMS: durationMS(average),
		P50MS: durationMS(
			percentile(sorted, 0.50),
		),
		P95MS: durationMS(
			percentile(sorted, 0.95),
		),
		P99MS: durationMS(
			percentile(sorted, 0.99),
		),
		MaxMS: durationMS(
			sorted[len(sorted)-1],
		),
	}
}

func percentile(
	sorted []time.Duration,
	p float64,
) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	if p <= 0 {
		return sorted[0]
	}

	if p >= 1 {
		return sorted[len(sorted)-1]
	}

	position :=
		p * float64(len(sorted))

	index :=
		int(math.Ceil(position)) - 1

	if index < 0 {
		index = 0
	}

	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

func durationMS(
	value time.Duration,
) float64 {
	return float64(value) /
		float64(time.Millisecond)
}
