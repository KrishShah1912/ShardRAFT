package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/KrishShah1912/shardraft/internal/benchmark"
)

func main() {
	cfg := parseFlags()

	if err := benchmark.ValidateConfig(cfg); err != nil {
		fmt.Fprintf(
			os.Stderr,
			"configuration error: %v\n",
			err,
		)
		os.Exit(2)
	}

	fmt.Println()
	fmt.Println("ShardRAFT Benchmark")
	fmt.Println("===================")
	fmt.Println()
	fmt.Printf(
		"Requested target : %s\n",
		cfg.Target,
	)
	fmt.Println(
		"Discovering leader and preparing benchmark...",
	)

	result, err := benchmark.Run(
		context.Background(),
		cfg,
	)

	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"benchmark failed: %v\n",
			err,
		)
		os.Exit(1)
	}

	printResult(result)

	if result.Failed > 0 {
		os.Exit(1)
	}
}

func parseFlags() benchmark.Config {
	cfg := benchmark.DefaultConfig()

	flag.StringVar(
		&cfg.Target,
		"target",
		cfg.Target,
		"any ShardRAFT node URL",
	)

	flag.IntVar(
		&cfg.Requests,
		"requests",
		cfg.Requests,
		"number of operations",
	)

	flag.IntVar(
		&cfg.Concurrency,
		"concurrency",
		cfg.Concurrency,
		"number of concurrent workers",
	)

	flag.Float64Var(
		&cfg.ReadRatio,
		"read-ratio",
		cfg.ReadRatio,
		"fraction of operations that are reads",
	)

	flag.IntVar(
		&cfg.ValueSize,
		"value-size",
		cfg.ValueSize,
		"value size in bytes",
	)

	flag.DurationVar(
		&cfg.Timeout,
		"timeout",
		cfg.Timeout,
		"HTTP request timeout",
	)

	flag.Parse()

	return cfg
}

func printResult(
	result benchmark.Result,
) {
	fmt.Println()
	fmt.Println("Cluster")
	fmt.Println("-------")

	fmt.Printf(
		"Initial leader    : Node %d\n",
		result.InitialLeader.ID,
	)

	fmt.Printf(
		"Initial term      : %d\n",
		result.InitialLeader.Term,
	)

	fmt.Println()
	fmt.Println("Workload")
	fmt.Println("--------")

	fmt.Printf(
		"Requests          : %d\n",
		result.Requests,
	)

	fmt.Printf(
		"Concurrency       : %d\n",
		result.Concurrency,
	)

	fmt.Printf(
		"Read ratio        : %.1f%%\n",
		result.ReadRatio*100,
	)

	fmt.Printf(
		"Write ratio       : %.1f%%\n",
		(1-result.ReadRatio)*100,
	)

	fmt.Printf(
		"Value size        : %d bytes\n",
		result.ValueSize,
	)

	fmt.Println()
	fmt.Println("Results")
	fmt.Println("-------")

	fmt.Printf(
		"Successful        : %d\n",
		result.Successful,
	)

	fmt.Printf(
		"Failed            : %d\n",
		result.Failed,
	)

	fmt.Printf(
		"Reads             : %d\n",
		result.Reads,
	)

	fmt.Printf(
		"Writes            : %d\n",
		result.Writes,
	)

	fmt.Printf(
		"Elapsed           : %.0f ms\n",
		result.DurationMS,
	)

	fmt.Printf(
		"Throughput        : %.2f ops/sec\n",
		result.ThroughputOPS,
	)

	printLatency(
		"Overall Latency",
		result.OverallLatency,
	)

	printLatency(
		"Read Latency",
		result.ReadLatency,
	)

	printLatency(
		"Write Latency",
		result.WriteLatency,
	)

	if len(result.Errors) > 0 {
		fmt.Println()
		fmt.Println("Failures")
		fmt.Println("--------")

		messages :=
			make(
				[]string,
				0,
				len(result.Errors),
			)

		for message := range result.Errors {
			messages =
				append(
					messages,
					message,
				)
		}

		sort.Strings(messages)

		for _, message := range messages {

			fmt.Printf(
				"%d x %s\n",
				result.Errors[message],
				message,
			)
		}
	}

	if len(result.StatusCodes) > 0 {
		fmt.Println()
		fmt.Println("HTTP Status Codes")
		fmt.Println("-----------------")

		codes :=
			make(
				[]int,
				0,
				len(result.StatusCodes),
			)

		for code := range result.StatusCodes {
			codes =
				append(
					codes,
					code,
				)
		}

		sort.Ints(codes)

		for _, code := range codes {

			fmt.Printf(
				"%d              : %d\n",
				code,
				result.StatusCodes[code],
			)
		}
	}

	fmt.Println()
	fmt.Println("Final Cluster State")
	fmt.Println("-------------------")

	fmt.Printf(
		"Leader            : Node %d\n",
		result.FinalLeader.ID,
	)

	fmt.Printf(
		"Term              : %d\n",
		result.FinalLeader.Term,
	)

	fmt.Printf(
		"Commit index      : %d\n",
		result.FinalLeader.CommitIndex,
	)

	fmt.Printf(
		"Applied index     : %d\n",
		result.FinalLeader.AppliedIndex,
	)
}

func printLatency(
	title string,
	stats benchmark.LatencyStats,
) {
	fmt.Println()
	fmt.Println(title)
	fmt.Println(
		"----------------",
	)

	fmt.Printf(
		"Average           : %.3f ms\n",
		stats.AverageMS,
	)

	fmt.Printf(
		"p50               : %.3f ms\n",
		stats.P50MS,
	)

	fmt.Printf(
		"p95               : %.3f ms\n",
		stats.P95MS,
	)

	fmt.Printf(
		"p99               : %.3f ms\n",
		stats.P99MS,
	)

	fmt.Printf(
		"Max               : %.3f ms\n",
		stats.MaxMS,
	)
}
