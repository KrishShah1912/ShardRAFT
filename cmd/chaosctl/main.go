package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

type commandRunner func(
	context.Context,
	[]string,
) error

func usage(w io.Writer) {
	fmt.Fprintln(w, "ShardRAFT local chaos control")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  chaosctl status")
	fmt.Fprintln(w, "  chaosctl stop NODE")
	fmt.Fprintln(w, "  chaosctl start NODE")
	fmt.Fprintln(w, "  chaosctl restart NODE")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Valid nodes: n1, n2, n3, n4, n5")
}

func validNode(node string) bool {
	switch node {
	case "n1", "n2", "n3", "n4", "n5":
		return true
	default:
		return false
	}
}

func composeArgs(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("missing command")
	}

	base := []string{
		"compose",
		"-f",
		"docker-compose.yml",
		"-p",
		"shardraft",
	}

	switch args[0] {
	case "status":
		if len(args) != 1 {
			return nil, fmt.Errorf(
				"status does not accept a node",
			)
		}

		return append(base, "ps", "-a"), nil

	case "stop", "start", "restart":
		if len(args) != 2 {
			return nil, fmt.Errorf(
				"%s requires exactly one node",
				args[0],
			)
		}

		if !validNode(args[1]) {
			return nil, fmt.Errorf(
				"invalid storage node: %s",
				args[1],
			)
		}

		return append(base, args[0], args[1]), nil

	default:
		return nil, fmt.Errorf(
			"unknown command: %s",
			args[0],
		)
	}
}

func run(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	runner commandRunner,
) int {
	if len(args) == 1 {
		switch args[0] {
		case "help", "-h", "--help":
			usage(stdout)
			return 0
		}
	}

	command, err := composeArgs(args)

	if err != nil {
		fmt.Fprintln(stderr, err)
		usage(stderr)
		return 2
	}

	if err := runner(ctx, command); err != nil {
		fmt.Fprintf(
			stderr,
			"docker compose command failed: %v\n",
			err,
		)

		return 1
	}

	return 0
}

func main() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)

	defer cancel()

	runner := func(
		ctx context.Context,
		args []string,
	) error {
		cmd := exec.CommandContext(
			ctx,
			"docker",
			args...,
		)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		return cmd.Run()
	}

	code := run(
		ctx,
		os.Args[1:],
		os.Stdout,
		os.Stderr,
		runner,
	)

	cancel()
	os.Exit(code)
}
