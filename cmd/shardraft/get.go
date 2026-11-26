package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

const cliExitNotFound = 3

type cliGetResult struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Found    bool   `json:"found"`
	ReadMode string `json:"read_mode"`
}

func executeCLIGet(
	ctx context.Context,
	kv cliKVClient,
	invocation cliInvocation,
	stdout io.Writer,
	stderr io.Writer,
) int {
	command := invocation.Command

	result := cliGetResult{
		Key:      command.Key,
		ReadMode: "linearizable",
	}

	var err error

	if command.Local {
		result.ReadMode = "local"

		result.Value, result.Found, err = kv.GetLocal(
			ctx,
			command.Key,
		)
	} else {
		result.Value, result.Found, err = kv.Get(
			ctx,
			command.Key,
		)
	}

	if err != nil {
		fmt.Fprintf(stderr, "error: get failed: %v\n", err)
		return cliExitFailure
	}

	if !result.Found {
		result.Value = ""
	}

	if err := writeCLIGetResult(
		stdout,
		invocation.Config.Output,
		result,
	); err != nil {
		fmt.Fprintf(stderr, "error: output failed: %v\n", err)
		return cliExitFailure
	}

	if !result.Found {
		return cliExitNotFound
	}

	return cliExitSuccess
}

func writeCLIGetResult(
	output io.Writer,
	format string,
	result cliGetResult,
) error {
	switch format {
	case "json":
		return json.NewEncoder(output).Encode(result)

	case "text":
		if !result.Found {
			_, err := fmt.Fprintf(
				output,
				"Key: %q\nFound: false\nRead mode: %s\n",
				result.Key,
				result.ReadMode,
			)

			return err
		}

		_, err := fmt.Fprintf(
			output,
			"Key: %q\nValue: %q\nFound: true\nRead mode: %s\n",
			result.Key,
			result.Value,
			result.ReadMode,
		)

		return err

	default:
		return fmt.Errorf("unsupported output format: %q", format)
	}
}
