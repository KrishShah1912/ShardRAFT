package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
)

const (
	cliExitSuccess = 0
	cliExitFailure = 1
	cliExitUsage   = 2
)

type cliExecutor func(
	context.Context,
	cliKVClient,
	cliInvocation,
	io.Writer,
	io.Writer,
) int

func runCLI(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	factory cliClientFactory,
	execute cliExecutor,
) (exitCode int) {
	var parseOutput bytes.Buffer

	invocation, err := parseCLIArgs(
		args,
		&parseOutput,
	)

	if errors.Is(err, flag.ErrHelp) {
		if _, writeErr := io.Copy(stdout, &parseOutput); writeErr != nil {
			return cliExitFailure
		}

		return cliExitSuccess
	}

	if err != nil {
		if parseOutput.Len() > 0 {
			_, _ = io.Copy(stderr, &parseOutput)
		} else {
			fmt.Fprintf(stderr, "error: %v\n", err)
		}

		return cliExitUsage
	}

	if factory == nil || execute == nil {
		fmt.Fprintln(stderr, "error: invalid CLI execution configuration")
		return cliExitFailure
	}

	operationCtx, cancel := context.WithTimeout(
		ctx,
		invocation.Config.Timeout,
	)

	defer cancel()

	if err := operationCtx.Err(); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return cliExitFailure
	}

	kv, err := factory(invocation.Config)

	if err != nil {
		fmt.Fprintf(stderr, "error: client initialization failed: %v\n", err)
		return cliExitFailure
	}

	if kv == nil {
		fmt.Fprintln(stderr, "error: client initialization returned no client")
		return cliExitFailure
	}

	defer func() {
		if err := kv.Close(); err != nil {
			fmt.Fprintf(stderr, "error: client cleanup failed: %v\n", err)

			if exitCode == cliExitSuccess {
				exitCode = cliExitFailure
			}
		}
	}()

	if err := operationCtx.Err(); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return cliExitFailure
	}

	return execute(
		operationCtx,
		kv,
		invocation,
		stdout,
		stderr,
	)
}
