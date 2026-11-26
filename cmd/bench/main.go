package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func run() int {
	cfg, err := parseConfig(os.Args[1:], os.Stderr)

	if errors.Is(err, flag.ErrHelp) {
		return 0
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	defer cancel()

	fmt.Println("ShardRAFT Multi-Raft Benchmark")
	fmt.Println("==============================")

	fmt.Printf("Distribution: %s\n", cfg.Distribution)
	fmt.Printf("Requests: %d\n", cfg.Requests)
	fmt.Printf("Concurrency: %d\n", cfg.Concurrency)
	fmt.Printf("Keys: %d\n", cfg.Keys)
	fmt.Printf("Read ratio: %.2f\n", cfg.ReadRatio)
	fmt.Printf("Value size: %d bytes\n", cfg.ValueSize)

	result, runErr := runBenchmark(ctx, cfg)

	if result.RunID == "" {
		fmt.Fprintln(os.Stderr, runErr)
		return 1
	}

	summary := summarize(result)

	if _, err := saveResults(result, summary); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	fmt.Printf("Successful: %d\n", summary.Successful)
	fmt.Printf("Failed: %d\n", summary.Failed)
	fmt.Printf("Unknown: %d\n", summary.Unknown)
	fmt.Printf("Pending: %d\n", summary.Pending)
	fmt.Printf("Reads: %d\n", summary.Reads)
	fmt.Printf("Writes: %d\n", summary.Writes)
	fmt.Printf("Elapsed: %.3f ms\n", summary.DurationMS)
	fmt.Printf("Throughput: %.2f ops/sec\n", summary.ThroughputOPS)
	fmt.Printf("Average: %.3f ms\n", summary.AverageMS)
	fmt.Printf("p50: %.3f ms\n", summary.P50MS)
	fmt.Printf("p95: %.3f ms\n", summary.P95MS)
	fmt.Printf("p99: %.3f ms\n", summary.P99MS)

	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		return 1
	}

	if summary.Successful != cfg.Requests {
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
