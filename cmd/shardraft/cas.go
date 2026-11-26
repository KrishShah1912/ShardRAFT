package main

import (
	"context"
	"fmt"
	"io"
)

func executeCLICAS(
	ctx context.Context,
	kv cliKVClient,
	invocation cliInvocation,
	stdout io.Writer,
	stderr io.Writer,
) int {
	identity, err := resolveCLIMutationIdentity(
		kv,
		invocation.Config,
	)

	if err != nil {
		fmt.Fprintf(
			stderr,
			"error: mutation identity initialization failed: %v\n",
			err,
		)

		return cliExitFailure
	}

	response, err := kv.CompareAndSwapWithIdentity(
		ctx,
		invocation.Command.Key,
		invocation.Command.Expected,
		invocation.Command.Value,
		identity,
	)

	if err != nil {
		return writeCLIMutationFailure(
			stderr,
			cliCAS,
			identity,
			err,
		)
	}

	return completeCLIMutation(
		invocation,
		identity,
		response,
		stdout,
		stderr,
	)
}
