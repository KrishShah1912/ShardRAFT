package main

import (
	"context"
	"fmt"
	"io"
)

func executeCLIPut(
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

	response, err := kv.PutWithIdentity(
		ctx,
		invocation.Command.Key,
		invocation.Command.Value,
		identity,
	)

	if err != nil {
		return writeCLIMutationFailure(
			stderr,
			cliPut,
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
