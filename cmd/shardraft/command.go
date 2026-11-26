package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

type cliOperation string

const (
	cliGet    cliOperation = "get"
	cliPut    cliOperation = "put"
	cliDelete cliOperation = "delete"
	cliCAS    cliOperation = "cas"
)

type cliCommand struct {
	Operation cliOperation
	Key       string
	Value     string
	Expected  string
	Local     bool
}

type cliInvocation struct {
	Config  cliConfig
	Command cliCommand
}

func writeCLIUsage(output io.Writer) {
	fmt.Fprintln(output, "ShardRAFT command-line client")
	fmt.Fprintln(output)
	fmt.Fprintln(output, "Usage:")
	fmt.Fprintln(output, "  shardraft [global flags] <command> [arguments]")
	fmt.Fprintln(output)
	fmt.Fprintln(output, "Commands:")
	fmt.Fprintln(output, "  get [--local] KEY")
	fmt.Fprintln(output, "  put KEY VALUE")
	fmt.Fprintln(output, "  delete KEY")
	fmt.Fprintln(output, "  cas KEY EXPECTED VALUE")
	fmt.Fprintln(output)
	fmt.Fprintln(output, "Global flags must appear before the command.")
	fmt.Fprintln(output, "Use -- before positional arguments beginning with '-'.")
	fmt.Fprintln(output, "GET uses linearizable reads by default.")
	fmt.Fprintln(output, "Local reads may return stale data.")
}

func parseCLIArgs(
	args []string,
	output io.Writer,
) (cliInvocation, error) {
	cfg := defaultCLIConfig()

	flags := flag.NewFlagSet(
		"shardraft",
		flag.ContinueOnError,
	)

	flags.SetOutput(output)

	endpoints := strings.Join(cfg.Endpoints, ",")

	flags.StringVar(
		&endpoints,
		"grpc-endpoints",
		endpoints,
		"comma-separated gRPC bootstrap endpoints",
	)

	flags.DurationVar(
		&cfg.Timeout,
		"timeout",
		cfg.Timeout,
		"overall operation timeout",
	)

	flags.StringVar(
		&cfg.Output,
		"output",
		cfg.Output,
		"output format: json or text",
	)

	flags.StringVar(
		&cfg.ClientID,
		"client-id",
		"",
		"explicit mutation client identity",
	)

	flags.StringVar(
		&cfg.RequestID,
		"request-id",
		"",
		"explicit mutation request identity",
	)

	flags.Usage = func() {
		writeCLIUsage(output)
		fmt.Fprintln(output)
		fmt.Fprintln(output, "Global flags:")
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return cliInvocation{}, err
	}

	remaining := flags.Args()

	if len(remaining) == 0 {
		flags.Usage()
		return cliInvocation{}, errors.New("command is required")
	}

	if remaining[0] == "help" && len(remaining) == 1 {
		flags.Usage()
		return cliInvocation{}, flag.ErrHelp
	}

	parsedEndpoints, err := parseCLIEndpoints(endpoints)

	if err != nil {
		return cliInvocation{}, err
	}

	cfg.Endpoints = parsedEndpoints

	if err := cfg.Validate(); err != nil {
		return cliInvocation{}, err
	}

	command, err := parseCLICommand(
		remaining,
		output,
	)

	if err != nil {
		return cliInvocation{}, err
	}

	if command.Operation == cliGet &&
		(cfg.ClientID != "" || cfg.RequestID != "") {
		return cliInvocation{}, errors.New(
			"mutation identities are not supported by get",
		)
	}

	return cliInvocation{
		Config:  cfg,
		Command: command,
	}, nil
}

func parseCLICommand(
	args []string,
	output io.Writer,
) (cliCommand, error) {
	if len(args) == 0 {
		return cliCommand{}, errors.New("command is required")
	}

	command := cliCommand{
		Operation: cliOperation(args[0]),
	}

	var expectedArgs int
	var usage string

	switch command.Operation {
	case cliGet:
		expectedArgs = 1
		usage = "get [--local] KEY"

	case cliPut:
		expectedArgs = 2
		usage = "put KEY VALUE"

	case cliDelete:
		expectedArgs = 1
		usage = "delete KEY"

	case cliCAS:
		expectedArgs = 3
		usage = "cas KEY EXPECTED VALUE"

	default:
		return cliCommand{}, fmt.Errorf(
			"unknown command: %q",
			args[0],
		)
	}

	flags := flag.NewFlagSet(
		string(command.Operation),
		flag.ContinueOnError,
	)

	flags.SetOutput(output)

	if command.Operation == cliGet {
		flags.BoolVar(
			&command.Local,
			"local",
			false,
			"allow potentially stale local reads",
		)
	}

	flags.Usage = func() {
		fmt.Fprintf(output, "Usage: shardraft %s\n", usage)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args[1:]); err != nil {
		return cliCommand{}, err
	}

	positional := flags.Args()

	if len(positional) != expectedArgs {
		return cliCommand{}, fmt.Errorf(
			"invalid arguments; usage: shardraft %s",
			usage,
		)
	}

	command.Key = positional[0]

	if command.Key == "" || !utf8.ValidString(command.Key) {
		return cliCommand{}, errors.New(
			"key must be non-empty valid UTF-8",
		)
	}

	switch command.Operation {
	case cliPut:
		command.Value = positional[1]

	case cliCAS:
		command.Expected = positional[1]
		command.Value = positional[2]
	}

	if !utf8.ValidString(command.Value) ||
		!utf8.ValidString(command.Expected) {
		return cliCommand{}, errors.New(
			"values must contain valid UTF-8",
		)
	}

	return command, nil
}
