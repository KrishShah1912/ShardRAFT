package main

import (
	"reflect"
	"testing"
	"time"
)

func TestCLIDefaultConfig(t *testing.T) {
	cfg := defaultCLIConfig()

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"127.0.0.1:9091",
		"127.0.0.1:9092",
		"127.0.0.1:9093",
	}

	if !reflect.DeepEqual(cfg.Endpoints, expected) {
		t.Fatalf("unexpected endpoints: %v", cfg.Endpoints)
	}

	if cfg.Timeout != 11*time.Second {
		t.Fatalf("unexpected timeout: %v", cfg.Timeout)
	}

	if cfg.Output != "json" {
		t.Fatalf("unexpected output: %s", cfg.Output)
	}

	if cfg.ClientID != "" || cfg.RequestID != "" {
		t.Fatal("default identities should be empty")
	}
}

func TestCLIParseEndpoints(t *testing.T) {
	endpoints, err := parseCLIEndpoints(
		"127.0.0.1:9091, localhost:9092,127.0.0.1:9091,[::1]:9093",
	)

	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"127.0.0.1:9091",
		"localhost:9092",
		"[::1]:9093",
	}

	if !reflect.DeepEqual(endpoints, expected) {
		t.Fatalf("got %v, want %v", endpoints, expected)
	}
}

func TestCLIInvalidEndpoints(t *testing.T) {
	cases := []string{
		"",
		"localhost",
		":9091",
		"localhost:0",
		"localhost:-1",
		"localhost:65536",
		"localhost:abc",
		"localhost:+9091",
		"0.0.0.0:9091",
		"[::]:9091",
		"*:9091",
		"http://localhost:9091",
		"invalid host:9091",
		"localhost:9091,",
		",localhost:9091",
		"localhost:9091,,localhost:9092",
	}

	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, err := parseCLIEndpoints(raw); err == nil {
				t.Fatalf("expected rejection of %q", raw)
			}
		})
	}
}

func TestCLIConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		change func(*cliConfig)
	}{
		{
			"NoEndpoints",
			func(cfg *cliConfig) {
				cfg.Endpoints = nil
			},
		},
		{
			"InvalidEndpoint",
			func(cfg *cliConfig) {
				cfg.Endpoints = []string{"invalid"}
			},
		},
		{
			"ZeroTimeout",
			func(cfg *cliConfig) {
				cfg.Timeout = 0
			},
		},
		{
			"NegativeTimeout",
			func(cfg *cliConfig) {
				cfg.Timeout = -time.Second
			},
		},
		{
			"InvalidOutput",
			func(cfg *cliConfig) {
				cfg.Output = "xml"
			},
		},
		{
			"RequestWithoutClient",
			func(cfg *cliConfig) {
				cfg.RequestID = "request-1"
			},
		},
		{
			"InvalidClientUTF8",
			func(cfg *cliConfig) {
				cfg.ClientID = string([]byte{0xff})
			},
		},
		{
			"InvalidRequestUTF8",
			func(cfg *cliConfig) {
				cfg.ClientID = "client-1"
				cfg.RequestID = string([]byte{0xff})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultCLIConfig()
			tc.change(&cfg)

			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestCLIValidExplicitIdentity(t *testing.T) {
	cfg := defaultCLIConfig()
	cfg.ClientID = "client-1"
	cfg.RequestID = "request-1"
	cfg.Output = "text"

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCLIClientIdentityWithoutRequest(t *testing.T) {
	cfg := defaultCLIConfig()
	cfg.ClientID = "client-1"

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCLIConfigIndependentDefaults(t *testing.T) {
	first := defaultCLIConfig()
	second := defaultCLIConfig()

	first.Endpoints[0] = "localhost:9999"

	if second.Endpoints[0] != "127.0.0.1:9091" {
		t.Fatal("default endpoint slices must be independent")
	}
}
