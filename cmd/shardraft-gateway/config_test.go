package main

import (
	"reflect"
	"testing"
)

func TestGatewayDefaultConfig(t *testing.T) {
	cfg := defaultGatewayConfig()

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	if len(cfg.Endpoints) != 3 {
		t.Fatalf("expected 3 endpoints, got %d", len(cfg.Endpoints))
	}
}

func TestGatewayParseEndpoints(t *testing.T) {
	endpoints, err := parseGatewayEndpoints(
		"127.0.0.1:9091, 127.0.0.1:9092,127.0.0.1:9091",
	)

	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"127.0.0.1:9091",
		"127.0.0.1:9092",
	}

	if !reflect.DeepEqual(endpoints, expected) {
		t.Fatalf("got %v, want %v", endpoints, expected)
	}
}

func TestGatewayInvalidEndpoints(t *testing.T) {
	cases := []string{
		"",
		"localhost",
		"localhost:0",
		"localhost:65536",
		"localhost:abc",
		":9091",
		"*:9091",
		"0.0.0.0:9091",
		"[::]:9091",
		"127.0.0.1:9091,",
		"http://localhost:9091",
	}

	for _, value := range cases {
		t.Run(value, func(t *testing.T) {
			if _, err := parseGatewayEndpoints(value); err == nil {
				t.Fatalf("expected error for %q", value)
			}
		})
	}
}

func TestGatewayInvalidConfig(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*gatewayConfig)
	}{
		{
			name: "InvalidListenAddress",
			modify: func(cfg *gatewayConfig) {
				cfg.ListenAddress = "invalid"
			},
		},
		{
			name: "MissingEndpoints",
			modify: func(cfg *gatewayConfig) {
				cfg.Endpoints = nil
			},
		},
		{
			name: "InvalidEndpoint",
			modify: func(cfg *gatewayConfig) {
				cfg.Endpoints = []string{"invalid"}
			},
		},
		{
			name: "InvalidTimeout",
			modify: func(cfg *gatewayConfig) {
				cfg.RequestTimeout = 0
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultGatewayConfig()
			tc.modify(&cfg)

			if err := cfg.Validate(); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestGatewayClientCreation(t *testing.T) {
	cfg := defaultGatewayConfig()

	c, err := newGatewayClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if c.ClientID() == "" {
		t.Fatal("expected nonempty client identity")
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayClientInvalidConfig(t *testing.T) {
	cfg := defaultGatewayConfig()
	cfg.Endpoints = nil

	c, err := newGatewayClient(cfg)

	if err == nil || c != nil {
		t.Fatal("expected client creation to fail")
	}
}
