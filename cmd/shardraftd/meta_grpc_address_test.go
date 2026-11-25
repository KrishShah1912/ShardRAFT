package main

import (
	"net"
	"testing"
)

type metaTestAddress string

func (a metaTestAddress) Network() string {
	return "tcp"
}

func (a metaTestAddress) String() string {
	return string(a)
}

func TestMetaGRPCLocalAddress(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		bound      net.Addr
		expected   string
	}{
		{
			"IPv4FixedPort",
			"127.0.0.1:9091",
			metaTestAddress("127.0.0.1:9091"),
			"127.0.0.1:9091",
		},
		{
			"IPv4DynamicPort",
			"127.0.0.1:0",
			metaTestAddress("127.0.0.1:49152"),
			"127.0.0.1:49152",
		},
		{
			"Hostname",
			"localhost:0",
			metaTestAddress("127.0.0.1:49153"),
			"localhost:49153",
		},
		{
			"IPv6",
			"[::1]:0",
			metaTestAddress("[::1]:49154"),
			"[::1]:49154",
		},
		{
			"IPv6Zone",
			"[fe80::1%en0]:0",
			metaTestAddress("[fe80::1%en0]:49155"),
			"[fe80::1%en0]:49155",
		},
		{
			"IPv4Wildcard",
			"0.0.0.0:9091",
			metaTestAddress("[::]:9091"),
			"",
		},
		{
			"IPv6Wildcard",
			"[::]:9091",
			metaTestAddress("[::]:9091"),
			"",
		},
		{
			"EmptyHost",
			":9091",
			metaTestAddress("[::]:9091"),
			"",
		},
		{
			"WildcardHost",
			"*:9091",
			metaTestAddress("[::]:9091"),
			"",
		},
		{
			"InvalidConfiguredAddress",
			"invalid-address",
			metaTestAddress("127.0.0.1:9091"),
			"",
		},
		{
			"InvalidBoundAddress",
			"127.0.0.1:9091",
			metaTestAddress("invalid-address"),
			"",
		},
		{
			"ZeroBoundPort",
			"127.0.0.1:0",
			metaTestAddress("127.0.0.1:0"),
			"",
		},
		{
			"InvalidBoundPort",
			"127.0.0.1:0",
			metaTestAddress("127.0.0.1:65536"),
			"",
		},
		{
			"NonNumericBoundPort",
			"127.0.0.1:0",
			metaTestAddress("127.0.0.1:invalid"),
			"",
		},
		{
			"NilBoundAddress",
			"127.0.0.1:9091",
			nil,
			"",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := metaGRPCLocalAddress(
				tc.configured,
				tc.bound,
			)

			if actual != tc.expected {
				t.Fatalf(
					"address=%q want=%q",
					actual,
					tc.expected,
				)
			}
		})
	}
}
