package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCLIParseCommands(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want cliCommand
	}{
		{
			name: "Get",
			args: []string{"get", "balance"},
			want: cliCommand{
				Operation: cliGet,
				Key:       "balance",
			},
		},
		{
			name: "LocalGet",
			args: []string{"get", "--local", "balance"},
			want: cliCommand{
				Operation: cliGet,
				Key:       "balance",
				Local:     true,
			},
		},
		{
			name: "Put",
			args: []string{"put", "balance", "100"},
			want: cliCommand{
				Operation: cliPut,
				Key:       "balance",
				Value:     "100",
			},
		},
		{
			name: "Delete",
			args: []string{"delete", "balance"},
			want: cliCommand{
				Operation: cliDelete,
				Key:       "balance",
			},
		},
		{
			name: "CAS",
			args: []string{"cas", "balance", "100", "200"},
			want: cliCommand{
				Operation: cliCAS,
				Key:       "balance",
				Expected:  "100",
				Value:     "200",
			},
		},
		{
			name: "EmptyValue",
			args: []string{"put", "key", ""},
			want: cliCommand{
				Operation: cliPut,
				Key:       "key",
			},
		},
		{
			name: "EmptyCASValues",
			args: []string{"cas", "key", "", ""},
			want: cliCommand{
				Operation: cliCAS,
				Key:       "key",
			},
		},
		{
			name: "Unicode",
			args: []string{"put", "ключ", "你好"},
			want: cliCommand{
				Operation: cliPut,
				Key:       "ключ",
				Value:     "你好",
			},
		},
		{
			name: "DashKey",
			args: []string{"get", "--", "-key"},
			want: cliCommand{
				Operation: cliGet,
				Key:       "-key",
			},
		},
		{
			name: "DashValue",
			args: []string{"put", "key", "-100"},
			want: cliCommand{
				Operation: cliPut,
				Key:       "key",
				Value:     "-100",
			},
		},
		{
			name: "Spaces",
			args: []string{"put", "my key", "hello world"},
			want: cliCommand{
				Operation: cliPut,
				Key:       "my key",
				Value:     "hello world",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			invocation, err := parseCLIArgs(
				tc.args,
				io.Discard,
			)

			if err != nil {
				t.Fatal(err)
			}

			if invocation.Command != tc.want {
				t.Fatalf(
					"got %+v, want %+v",
					invocation.Command,
					tc.want,
				)
			}

			if err := invocation.Config.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCLIGlobalFlags(t *testing.T) {
	invocation, err := parseCLIArgs(
		[]string{
			"--grpc-endpoints",
			"localhost:9091,localhost:9092,localhost:9091",
			"--timeout", "5s",
			"--output", "text",
			"--client-id", "client-1",
			"--request-id", "request-1",
			"put", "balance", "100",
		},
		io.Discard,
	)

	if err != nil {
		t.Fatal(err)
	}

	cfg := invocation.Config

	if !reflect.DeepEqual(
		cfg.Endpoints,
		[]string{"localhost:9091", "localhost:9092"},
	) {
		t.Fatalf("unexpected endpoints: %v", cfg.Endpoints)
	}

	if cfg.Timeout != 5*time.Second ||
		cfg.Output != "text" ||
		cfg.ClientID != "client-1" ||
		cfg.RequestID != "request-1" {
		t.Fatalf("unexpected configuration: %+v", cfg)
	}
}

func TestCLIInvalidCommands(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})

	cases := []struct {
		name string
		args []string
	}{
		{"MissingCommand", nil},
		{"UnknownCommand", []string{"unknown"}},
		{"MissingGetKey", []string{"get"}},
		{"ExtraGetKey", []string{"get", "a", "b"}},
		{"MissingPutValue", []string{"put", "key"}},
		{"ExtraPutValue", []string{"put", "key", "v", "extra"}},
		{"MissingDeleteKey", []string{"delete"}},
		{"ExtraDeleteKey", []string{"delete", "a", "b"}},
		{"MissingCASValue", []string{"cas", "key", "old"}},
		{"ExtraCASValue", []string{"cas", "key", "a", "b", "c"}},
		{"EmptyKey", []string{"get", ""}},
		{"InvalidKeyUTF8", []string{"get", invalidUTF8}},
		{"InvalidValueUTF8", []string{"put", "key", invalidUTF8}},
		{"InvalidExpectedUTF8", []string{"cas", "key", invalidUTF8, "v"}},
		{"InvalidCASValueUTF8", []string{"cas", "key", "old", invalidUTF8}},
		{"LocalPut", []string{"put", "--local", "key", "value"}},
		{"LocalDelete", []string{"delete", "--local", "key"}},
		{"LocalCAS", []string{"cas", "--local", "key", "a", "b"}},
		{"UnknownGlobalFlag", []string{"--unknown", "get", "key"}},
		{"UnknownCommandFlag", []string{"get", "--unknown", "key"}},
		{"InvalidDuration", []string{"--timeout", "invalid", "get", "key"}},
		{"ZeroDuration", []string{"--timeout", "0s", "get", "key"}},
		{"NegativeDuration", []string{"--timeout", "-1s", "get", "key"}},
		{"InvalidOutput", []string{"--output", "xml", "get", "key"}},
		{"InvalidEndpoint", []string{"--grpc-endpoints", "invalid", "get", "key"}},
		{"MissingFlagValue", []string{"--timeout"}},
		{"RequestWithoutClient", []string{"--request-id", "r1", "put", "k", "v"}},
		{"GetWithClientID", []string{"--client-id", "c1", "get", "k"}},
		{"GetWithIdentity", []string{"--client-id", "c1", "--request-id", "r1", "get", "k"}},
		{"GlobalFlagAfterCommand", []string{"get", "--timeout", "5s", "key"}},
		{"InvalidLocalValue", []string{"get", "--local=invalid", "key"}},
		{"LocalAfterKey", []string{"get", "key", "--local"}},
		{"ExtraHelpArgument", []string{"help", "extra"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseCLIArgs(
				tc.args,
				io.Discard,
			)

			if err == nil || errors.Is(err, flag.ErrHelp) {
				t.Fatalf("expected argument error, got %v", err)
			}
		})
	}
}

func TestCLIHelp(t *testing.T) {
	cases := [][]string{
		{"--help"},
		{"-h"},
		{"help"},
		{"get", "--help"},
		{"put", "--help"},
		{"delete", "--help"},
		{"cas", "--help"},
	}

	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			var output bytes.Buffer

			_, err := parseCLIArgs(args, &output)

			if !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("expected help, got %v", err)
			}

			if !strings.Contains(output.String(), "Usage:") {
				t.Fatalf("missing usage: %s", output.String())
			}
		})
	}
}

func TestCLIDefaultReadMode(t *testing.T) {
	invocation, err := parseCLIArgs(
		[]string{"get", "key"},
		io.Discard,
	)

	if err != nil {
		t.Fatal(err)
	}

	if invocation.Command.Local {
		t.Fatal("default read must be linearizable")
	}
}

func TestCLIExplicitLinearizableRead(t *testing.T) {
	invocation, err := parseCLIArgs(
		[]string{"get", "--local=false", "key"},
		io.Discard,
	)

	if err != nil {
		t.Fatal(err)
	}

	if invocation.Command.Local {
		t.Fatal("expected linearizable read")
	}
}

func TestCLIClientOnlyIdentity(t *testing.T) {
	invocation, err := parseCLIArgs(
		[]string{
			"--client-id", "client-1",
			"put", "key", "value",
		},
		io.Discard,
	)

	if err != nil {
		t.Fatal(err)
	}

	if invocation.Config.ClientID != "client-1" ||
		invocation.Config.RequestID != "" {
		t.Fatal("unexpected identity configuration")
	}
}

func TestCLIParseEmptyCommand(t *testing.T) {
	_, err := parseCLICommand(nil, io.Discard)

	if err == nil {
		t.Fatal("expected missing command error")
	}
}

func TestCLIParserIndependentInvocations(t *testing.T) {
	first, err := parseCLIArgs(
		[]string{
			"--output", "text",
			"get", "--local", "key",
		},
		io.Discard,
	)

	if err != nil {
		t.Fatal(err)
	}

	second, err := parseCLIArgs(
		[]string{"get", "key"},
		io.Discard,
	)

	if err != nil {
		t.Fatal(err)
	}

	if first.Config.Output != "text" ||
		!first.Command.Local {
		t.Fatal("first invocation lost its configuration")
	}

	if second.Config.Output != "json" ||
		second.Command.Local {
		t.Fatal("configuration leaked between invocations")
	}
}
