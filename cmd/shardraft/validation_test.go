package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

type cliValidationFailWriter struct{}

func (cliValidationFailWriter) Write(
	p []byte,
) (int, error) {
	return 0, errors.New("output unavailable")
}

func TestCLIValidationHelpOutputFailure(t *testing.T) {
	var stderr bytes.Buffer

	factoryCalls := 0

	code := runCLI(
		context.Background(),
		[]string{"--help"},
		cliValidationFailWriter{},
		&stderr,
		func(cliConfig) (cliKVClient, error) {
			factoryCalls++
			return nil, errors.New("unexpected initialization")
		},
		executeCLICommand,
	)

	if code != cliExitFailure {
		t.Fatalf("exit=%d want=%d", code, cliExitFailure)
	}

	if factoryCalls != 0 {
		t.Fatal("help initialized the client")
	}
}

func TestCLIValidationExpiredParentDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)

	defer cancel()

	var stdout, stderr bytes.Buffer

	factoryCalls := 0

	code := runCLI(
		ctx,
		[]string{"get", "balance"},
		&stdout,
		&stderr,
		func(cliConfig) (cliKVClient, error) {
			factoryCalls++
			return nil, errors.New("unexpected initialization")
		},
		executeCLICommand,
	)

	if code != cliExitFailure {
		t.Fatalf("unexpected exit code: %d", code)
	}

	if factoryCalls != 0 {
		t.Fatal("expired context initialized the client")
	}

	if stdout.Len() != 0 || stderr.Len() == 0 {
		t.Fatal("incorrect output for expired deadline")
	}
}

func TestCLIValidationInvalidArgumentsNeverInitialize(t *testing.T) {
	cases := [][]string{
		{},
		{"unknown"},
		{"get"},
		{"get", ""},
		{"get", "one", "two"},
		{"put", "key"},
		{"put", "", "value"},
		{"delete"},
		{"delete", ""},
		{"cas", "key", "expected"},
		{"cas", "", "expected", "value"},
		{"--timeout", "-1s", "get", "key"},
		{"--timeout", "0s", "get", "key"},
		{"--timeout", "invalid", "get", "key"},
		{"--output", "xml", "get", "key"},
		{"--grpc-endpoints", "", "get", "key"},
		{"--grpc-endpoints", "localhost:0", "get", "key"},
		{"--grpc-endpoints", "localhost:65536", "get", "key"},
		{"--request-id", "request", "put", "key", "value"},
		{"--client-id", "client", "get", "key"},
		{"put", "key", string([]byte{0xff})},
		{"get", string([]byte{0xff})},
	}

	for _, args := range cases {
		var stdout, stderr bytes.Buffer

		factoryCalls := 0

		code := runCLI(
			context.Background(),
			args,
			&stdout,
			&stderr,
			func(cliConfig) (cliKVClient, error) {
				factoryCalls++
				return nil, errors.New("unexpected initialization")
			},
			executeCLICommand,
		)

		if code != cliExitUsage {
			t.Fatalf(
				"args=%q exit=%d stderr=%q",
				args,
				code,
				stderr.String(),
			)
		}

		if factoryCalls != 0 {
			t.Fatalf("invalid arguments initialized client: %q", args)
		}

		if stdout.Len() != 0 || stderr.Len() == 0 {
			t.Fatalf("incorrect usage output: %q", args)
		}
	}

	t.Logf("validated %d invalid argument combinations", len(cases))
}

func TestCLIValidationSupportedEndpointForms(t *testing.T) {
	cases := []string{
		"127.0.0.1:9091",
		"localhost:9091",
		"node.example.com:9091",
		"[::1]:9091",
		"[2001:db8::1]:9091",
		"localhost:1",
		"localhost:65535",
	}

	for _, endpoint := range cases {
		cfg := defaultCLIConfig()
		cfg.Endpoints = []string{endpoint}

		if err := cfg.Validate(); err != nil {
			t.Fatalf(
				"valid endpoint rejected: %q: %v",
				endpoint,
				err,
			)
		}
	}
}

func TestCLIValidationRejectedEndpointForms(t *testing.T) {
	cases := []string{
		"",
		"localhost",
		":9091",
		"localhost:0",
		"localhost:-1",
		"localhost:65536",
		"localhost:abc",
		"0.0.0.0:9091",
		"[::]:9091",
		"bad host:9091",
		"http://localhost:9091",
		"localhost/path:9091",
	}

	for _, endpoint := range cases {
		cfg := defaultCLIConfig()
		cfg.Endpoints = []string{endpoint}

		if err := cfg.Validate(); err == nil {
			t.Fatalf("invalid endpoint accepted: %q", endpoint)
		}
	}
}

func TestCLIValidationValidCommands(t *testing.T) {
	cases := [][]string{
		{"get", "key"},
		{"get", "--local", "key"},
		{"put", "key", ""},
		{"put", "key", "你好"},
		{"put", "key", "line1\nline2"},
		{"delete", "key"},
		{"cas", "key", "", ""},
		{"cas", "key", "100", "200"},
		{"get", "--", "-key"},
		{"put", "--", "-key", "-value"},
		{"--output", "text", "get", "key"},
		{"--timeout", "1ms", "get", "key"},
		{
			"--client-id", "client",
			"--request-id", "request",
			"put", "key", "value",
		},
	}

	for _, args := range cases {
		invocation, err := parseCLIArgs(args, io.Discard)

		if err != nil {
			t.Fatalf("valid command rejected: %q: %v", args, err)
		}

		if err := invocation.Config.Validate(); err != nil {
			t.Fatalf("invalid parsed configuration: %v", err)
		}

		if invocation.Command.Key == "" {
			t.Fatal("valid command returned empty key")
		}
	}

	t.Logf("validated %d valid command combinations", len(cases))
}

func FuzzCLIValidationCommandParser(f *testing.F) {
	f.Add("get", "key", "", "")
	f.Add("put", "key", "value", "")
	f.Add("delete", "key", "", "")
	f.Add("cas", "key", "old", "new")
	f.Add("get", "--local", "key", "")
	f.Add("unknown", "", "", "")
	f.Add("put", "你好", "世界", "")
	f.Add("--timeout", "invalid", "get", "key")

	f.Fuzz(func(
		t *testing.T,
		a string,
		b string,
		c string,
		d string,
	) {
		args := []string{a, b, c, d}

		invocation, err := parseCLIArgs(args, io.Discard)

		if err != nil {
			return
		}

		if err := invocation.Config.Validate(); err != nil {
			t.Fatalf("parser accepted invalid configuration: %v", err)
		}

		if invocation.Command.Key == "" {
			t.Fatal("parser accepted empty key")
		}

		switch invocation.Command.Operation {
		case cliGet, cliPut, cliDelete, cliCAS:
		default:
			t.Fatalf(
				"parser accepted invalid operation: %q",
				invocation.Command.Operation,
			)
		}

		if invocation.Command.Local &&
			invocation.Command.Operation != cliGet {
			t.Fatal("local read enabled for mutation")
		}
	})
}

func FuzzCLIValidationEndpoints(f *testing.F) {
	f.Add("127.0.0.1:9091")
	f.Add("localhost:9091,localhost:9092")
	f.Add("[::1]:9091")
	f.Add("")
	f.Add("localhost:65536")
	f.Add("0.0.0.0:9091")
	f.Add("localhost:9091,localhost:9091")

	f.Fuzz(func(t *testing.T, input string) {
		endpoints, err := parseCLIEndpoints(input)

		if err != nil {
			return
		}

		if len(endpoints) == 0 {
			t.Fatal("accepted empty endpoint list")
		}

		seen := make(map[string]bool)

		for _, endpoint := range endpoints {
			if err := validateCLIEndpoint(endpoint); err != nil {
				t.Fatalf("accepted invalid endpoint: %v", err)
			}

			if seen[endpoint] {
				t.Fatalf("duplicate endpoint: %q", endpoint)
			}

			seen[endpoint] = true
		}

		cfg := defaultCLIConfig()
		cfg.Endpoints = endpoints

		if err := cfg.Validate(); err != nil {
			t.Fatalf("invalid parsed endpoints: %v", err)
		}
	})
}
