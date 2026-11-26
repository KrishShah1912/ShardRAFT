package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func parseGatewayFlags(
	args []string,
	output io.Writer,
) (gatewayConfig, error) {
	cfg := defaultGatewayConfig()

	flags := flag.NewFlagSet(
		"shardraft-gateway",
		flag.ContinueOnError,
	)
	flags.SetOutput(output)

	endpoints := strings.Join(cfg.Endpoints, ",")

	flags.StringVar(
		&cfg.ListenAddress,
		"http-addr",
		cfg.ListenAddress,
		"HTTP gateway listen address",
	)

	flags.StringVar(
		&endpoints,
		"grpc-endpoints",
		endpoints,
		"comma-separated gRPC bootstrap endpoints",
	)

	flags.DurationVar(
		&cfg.RequestTimeout,
		"request-timeout",
		cfg.RequestTimeout,
		"maximum backend operation duration",
	)

	if err := flags.Parse(args); err != nil {
		return cfg, err
	}

	if flags.NArg() != 0 {
		return cfg, errors.New(
			"unexpected positional arguments",
		)
	}

	parsed, err := parseGatewayEndpoints(endpoints)
	if err != nil {
		return cfg, err
	}

	cfg.Endpoints = parsed

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func main() {
	cfg, err := parseGatewayFlags(
		os.Args[1:],
		os.Stderr,
	)

	if errors.Is(err, flag.ErrHelp) {
		return
	}

	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	if err := runGateway(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
