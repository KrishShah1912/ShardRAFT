package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	code := runCLI(
		ctx,
		os.Args[1:],
		os.Stdout,
		os.Stderr,
		newCLIClient,
		executeCLICommand,
	)

	stop()

	os.Exit(code)
}
