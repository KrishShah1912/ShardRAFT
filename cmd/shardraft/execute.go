package main

import (
	"context"
	"fmt"
	"io"
)

func executeCLICommand(
	ctx context.Context,
	kv cliKVClient,
	invocation cliInvocation,
	stdout io.Writer,
	stderr io.Writer,
) int {
	switch invocation.Command.Operation {
	case cliGet:
		return executeCLIGet(
			ctx,
			kv,
			invocation,
			stdout,
			stderr,
		)

	case cliPut:
		return executeCLIPut(
			ctx,
			kv,
			invocation,
			stdout,
			stderr,
		)

	case cliDelete:
		return executeCLIDelete(
			ctx,
			kv,
			invocation,
			stdout,
			stderr,
		)

	case cliCAS:
		return executeCLICAS(
			ctx,
			kv,
			invocation,
			stdout,
			stderr,
		)

	default:
		fmt.Fprintf(
			stderr,
			"error: unsupported command %q\n",
			invocation.Command.Operation,
		)

		return cliExitFailure
	}
}
