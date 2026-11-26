package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type gatewayConfig struct {
	ListenAddress  string
	Endpoints      []string
	RequestTimeout time.Duration
}

func defaultGatewayConfig() gatewayConfig {
	return gatewayConfig{
		ListenAddress: "127.0.0.1:8085",
		Endpoints: []string{
			"127.0.0.1:9091",
			"127.0.0.1:9092",
			"127.0.0.1:9093",
		},
		RequestTimeout: 11 * time.Second,
	}
}

func validateGatewayAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", address, err)
	}

	if host == "" || strings.ContainsAny(host, " \t\r\n/?#") {
		return fmt.Errorf("invalid host in address %q", address)
	}

	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("invalid port in address %q", address)
	}

	return nil
}

func parseGatewayEndpoints(value string) ([]string, error) {
	var endpoints []string
	seen := make(map[string]bool)

	for _, item := range strings.Split(value, ",") {
		endpoint := strings.TrimSpace(item)

		if err := validateGatewayAddress(endpoint); err != nil {
			return nil, err
		}

		host, _, _ := net.SplitHostPort(endpoint)

		if host == "*" {
			return nil, errors.New("wildcard endpoint is not allowed")
		}

		if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
			return nil, errors.New("unspecified endpoint is not allowed")
		}

		if !seen[endpoint] {
			seen[endpoint] = true
			endpoints = append(endpoints, endpoint)
		}
	}

	return endpoints, nil
}

func (cfg gatewayConfig) Validate() error {
	if err := validateGatewayAddress(cfg.ListenAddress); err != nil {
		return fmt.Errorf("invalid gateway listen address: %w", err)
	}

	if len(cfg.Endpoints) == 0 {
		return errors.New("at least one gRPC endpoint is required")
	}

	for _, endpoint := range cfg.Endpoints {
		if _, err := parseGatewayEndpoints(endpoint); err != nil {
			return fmt.Errorf("invalid gRPC endpoint: %w", err)
		}
	}

	if cfg.RequestTimeout <= 0 {
		return errors.New("request timeout must be positive")
	}

	return nil
}
