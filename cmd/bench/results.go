package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/KrishShah1912/shardraft/internal/history"
)

type Summary struct {
	RunID         string  `json:"run_id"`
	Requests      int     `json:"requests"`
	Recorded      int     `json:"recorded"`
	Successful    int     `json:"successful"`
	Failed        int     `json:"failed"`
	Unknown       int     `json:"unknown"`
	Pending       int     `json:"pending"`
	Reads         int     `json:"reads"`
	Writes        int     `json:"writes"`
	Concurrency   int     `json:"concurrency"`
	Keys          int     `json:"keys"`
	ReadRatio     float64 `json:"read_ratio"`
	ValueSize     int     `json:"value_size"`
	Distribution  string  `json:"distribution"`
	ZipfS         float64 `json:"zipf_s"`
	Seed          int64   `json:"seed"`
	DurationMS    float64 `json:"duration_ms"`
	ThroughputOPS float64 `json:"throughput_ops"`
	AverageMS     float64 `json:"average_ms"`
	P50MS         float64 `json:"p50_ms"`
	P95MS         float64 `json:"p95_ms"`
	P99MS         float64 `json:"p99_ms"`
	MaxMS         float64 `json:"max_ms"`
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}

	index := int(math.Ceil(p*float64(len(sorted)))) - 1

	if index < 0 {
		index = 0
	}

	return sorted[index]
}

func summarize(result RunResult) Summary {
	cfg := result.Config

	s := Summary{
		RunID:        result.RunID,
		Requests:     cfg.Requests,
		Recorded:     len(result.History.Operations),
		Concurrency:  cfg.Concurrency,
		Keys:         cfg.Keys,
		ReadRatio:    cfg.ReadRatio,
		ValueSize:    cfg.ValueSize,
		Distribution: cfg.Distribution,
		ZipfS:        cfg.ZipfS,
		Seed:         cfg.Seed,
		DurationMS:   float64(result.Duration.Nanoseconds()) / 1e6,
	}

	var latencies []float64
	var total float64

	for _, op := range result.History.Operations {
		if op.Input.Kind == history.Get {
			s.Reads++
		} else {
			s.Writes++
		}

		switch op.Output.Outcome {
		case history.Success:
			s.Successful++

			ms := float64(op.ReturnNS-op.CallNS) / 1e6
			latencies = append(latencies, ms)
			total += ms

		case history.Failed:
			s.Failed++

		case history.Unknown:
			s.Unknown++

		case history.Pending:
			s.Pending++
		}
	}

	if result.Duration.Seconds() > 0 {
		s.ThroughputOPS =
			float64(s.Successful) / result.Duration.Seconds()
	}

	if len(latencies) > 0 {
		sort.Float64s(latencies)

		s.AverageMS = total / float64(len(latencies))
		s.P50MS = percentile(latencies, 0.50)
		s.P95MS = percentile(latencies, 0.95)
		s.P99MS = percentile(latencies, 0.99)
		s.MaxMS = latencies[len(latencies)-1]
	}

	return s
}

func saveResults(result RunResult, summary Summary) (string, error) {
	dir := filepath.Join(result.Config.OutputDir, result.RunID)

	if err := os.MkdirAll(result.Config.OutputDir, 0755); err != nil {
		return "", err
	}

	if err := os.Mkdir(dir, 0755); err != nil {
		return "", err
	}

	if err := history.Save(
		filepath.Join(dir, "history.json"),
		result.History,
	); err != nil {
		return dir, err
	}

	file, err := os.Create(filepath.Join(dir, "operations.csv"))

	if err != nil {
		return dir, err
	}

	defer file.Close()

	writer := csv.NewWriter(file)

	if err := writer.Write([]string{
		"run_id", "operation_id", "client_id", "request_id",
		"gateway", "operation", "key",
		"call_ns", "return_ns", "latency_ns",
		"input_bytes", "output_bytes",
		"outcome", "found", "applied", "error",
	}); err != nil {
		return dir, err
	}

	for _, op := range result.History.Operations {
		latency := ""

		if op.Output.Outcome != history.Pending {
			latency = strconv.FormatInt(op.ReturnNS-op.CallNS, 10)
		}

		if err := writer.Write([]string{
			result.RunID,
			strconv.FormatUint(op.ID, 10),
			op.Input.ClientID,
			op.Input.RequestID,
			result.Targets[op.ID],
			string(op.Input.Kind),
			op.Input.Key,
			strconv.FormatInt(op.CallNS, 10),
			strconv.FormatInt(op.ReturnNS, 10),
			latency,
			strconv.Itoa(len(op.Input.Value)),
			strconv.Itoa(len(op.Output.Value)),
			string(op.Output.Outcome),
			strconv.FormatBool(op.Output.Found),
			strconv.FormatBool(op.Output.Applied),
			op.Output.Error,
		}); err != nil {
			return dir, err
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return dir, err
	}

	if err := file.Close(); err != nil {
		return dir, err
	}

	data, err := json.MarshalIndent(summary, "", "  ")

	if err != nil {
		return dir, err
	}

	if err := os.WriteFile(
		filepath.Join(dir, "summary.json"),
		append(data, '\n'),
		0644,
	); err != nil {
		return dir, err
	}

	configData, err := json.MarshalIndent(result.Config, "", "  ")

	if err != nil {
		return dir, err
	}

	if err := os.WriteFile(
		filepath.Join(dir, "config.json"),
		append(configData, '\n'),
		0644,
	); err != nil {
		return dir, err
	}

	fmt.Printf("Results directory: %s\n", dir)

	return dir, nil
}
