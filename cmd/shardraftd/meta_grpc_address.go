package main

import (
	"net"
	"strconv"
	"strings"
)

// metaGRPCLocalAddress returns an advertisable local endpoint.
// Wildcard listeners do not identify a reachable client endpoint.
func metaGRPCLocalAddress(
	configuredAddress string,
	boundAddress net.Addr,
) string {
	if boundAddress == nil {
		return ""
	}

	host, _, err := net.SplitHostPort(configuredAddress)
	if err != nil {
		return ""
	}

	host = strings.TrimSpace(host)

	if host == "" || host == "*" {
		return ""
	}

	ipHost := host

	if index := strings.LastIndex(ipHost, "%"); index >= 0 {
		ipHost = ipHost[:index]
	}

	if ip := net.ParseIP(ipHost); ip != nil && ip.IsUnspecified() {
		return ""
	}

	_, port, err := net.SplitHostPort(boundAddress.String())
	if err != nil {
		return ""
	}

	number, err := strconv.Atoi(port)

	if err != nil || number <= 0 || number > 65535 {
		return ""
	}

	return net.JoinHostPort(host, port)
}
