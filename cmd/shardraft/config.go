package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type cliConfig struct {
	Endpoints []string
	Timeout   time.Duration
	Output    string
	ClientID  string
	RequestID string
}

func defaultCLIConfig() cliConfig {
	return cliConfig{
		Endpoints: []string{
			"127.0.0.1:9091",
			"127.0.0.1:9092",
			"127.0.0.1:9093",
		},
		Timeout: 11 * time.Second,
		Output:  "json",
	}
}

func validateCLIEndpoint(endpoint string) error {
	host, port, err := net.SplitHostPort(endpoint)

	if err != nil || host == "" {
		return fmt.Errorf("invalid gRPC endpoint: %q", endpoint)
	}

	if strings.ContainsAny(host, " \t\r\n/?#") {
		return fmt.Errorf("invalid endpoint host: %q", host)
	}

	if host == "*" {
		return fmt.Errorf("wildcard endpoint is not allowed")
	}

	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("unspecified endpoint is not allowed")
	}

	for _, character := range port {
		if character < '0' || character > '9' {
			return fmt.Errorf("invalid endpoint port: %q", port)
		}
	}

	number, err := strconv.Atoi(port)

	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("invalid endpoint port: %q", port)
	}

	return nil
}

func parseCLIEndpoints(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	endpoints := make([]string, 0, len(parts))
	seen := make(map[string]bool)

	for _, part := range parts {
		endpoint := strings.TrimSpace(part)

		if err := validateCLIEndpoint(endpoint); err != nil {
			return nil, err
		}

		if seen[endpoint] {
			continue
		}

		seen[endpoint] = true
		endpoints = append(endpoints, endpoint)
	}

	if len(endpoints) == 0 {
		return nil, fmt.Errorf("at least one gRPC endpoint is required")
	}

	return endpoints, nil
}

func (cfg cliConfig) Validate() error {
	if len(cfg.Endpoints) == 0 {
		return fmt.Errorf("at least one gRPC endpoint is required")
	}

	for _, endpoint := range cfg.Endpoints {
		if err := validateCLIEndpoint(endpoint); err != nil {
			return err
		}
	}

	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	switch cfg.Output {
	case "json", "text":
	default:
		return fmt.Errorf("output must be json or text")
	}

	if !utf8.ValidString(cfg.ClientID) ||
		!utf8.ValidString(cfg.RequestID) {
		return fmt.Errorf("request identities must contain valid UTF-8")
	}

	if cfg.RequestID != "" && cfg.ClientID == "" {
		return fmt.Errorf("request-id requires client-id")
	}

	return nil
}
