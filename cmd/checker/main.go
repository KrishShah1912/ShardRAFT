package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/KrishShah1912/shardraft/internal/history"
)

func run(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
) int {
	flags := flag.NewFlagSet(
		"checker",
		flag.ContinueOnError,
	)

	flags.SetOutput(stderr)

	path := flags.String(
		"history",
		"",
		"path to recorded history JSON",
	)

	assumeEmpty := flags.Bool(
		"assume-empty",
		false,
		"assert all recorded keys and request identities are initially unused",
	)

	timeout := flags.Duration(
		"timeout",
		30*time.Second,
		"total checker time budget",
	)

	jsonOutput := flags.Bool(
		"json",
		false,
		"print JSON report",
	)

	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}

		return 3
	}

	if *path == "" ||
		*timeout <= 0 ||
		flags.NArg() != 0 {

		fmt.Fprintln(
			stderr,
			"usage: checker -history FILE [-assume-empty] [-timeout 30s] [-json]",
		)

		return 3
	}

	h, err := history.Load(*path)

	if err != nil {
		fmt.Fprintf(stderr, "load history: %v\n", err)
		return 3
	}

	report, err := history.Check(
		h,
		history.CheckOptions{
			AssumeEmpty: *assumeEmpty,
			Timeout:     *timeout,
		},
	)

	if err != nil {
		fmt.Fprintf(stderr, "check history: %v\n", err)
		return 3
	}

	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")

		if err := encoder.Encode(report); err != nil {
			fmt.Fprintln(stderr, err)
			return 3
		}

	} else {
		fmt.Fprintln(stdout, "ShardRAFT Linearizability Checker")
		fmt.Fprintln(stdout, "================================")

		fmt.Fprintf(stdout, "Verdict: %s\n", report.Verdict)
		fmt.Fprintf(stdout, "Operations: %d\n", report.Operations)
		fmt.Fprintf(stdout, "Successful: %d\n", report.Successful)
		fmt.Fprintf(stdout, "Failed: %d\n", report.Failed)
		fmt.Fprintf(stdout, "Unknown: %d\n", report.Unknown)
		fmt.Fprintf(stdout, "Pending: %d\n", report.Pending)

		fmt.Fprintf(
			stdout,
			"Assume empty: %t\n",
			report.AssumeEmpty,
		)

		fmt.Fprintf(
			stdout,
			"Elapsed: %s\n",
			time.Duration(report.DurationNS),
		)

		if report.Reason != "" {
			fmt.Fprintf(stdout, "Reason: %s\n", report.Reason)
		}

		for _, key := range report.Keys {
			fmt.Fprintf(
				stdout,
				"Key %q: %s (%d operations)",
				key.Key,
				key.Verdict,
				key.Operations,
			)

			if key.Reason != "" {
				fmt.Fprintf(stdout, " - %s", key.Reason)
			}

			fmt.Fprintln(stdout)
		}
	}

	switch report.Verdict {
	case history.Linearizable:
		return 0

	case history.Illegal:
		return 1

	default:
		return 2
	}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
