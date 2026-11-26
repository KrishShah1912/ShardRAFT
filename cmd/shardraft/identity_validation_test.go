package main

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/client"
)

func TestCLIIdentityValidation(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})

	cases := []struct {
		name     string
		identity client.MutationIdentity
		valid    bool
	}{
		{
			name: "ValidASCII",
			identity: client.MutationIdentity{
				ClientID:  "client-1",
				RequestID: "request-1",
			},
			valid: true,
		},
		{
			name: "ValidUnicode",
			identity: client.MutationIdentity{
				ClientID:  "client-你好",
				RequestID: "request-世界",
			},
			valid: true,
		},
		{
			name: "ValidSpaces",
			identity: client.MutationIdentity{
				ClientID:  "client with spaces",
				RequestID: "request with spaces",
			},
			valid: true,
		},
		{
			name:     "BothEmpty",
			identity: client.MutationIdentity{},
		},
		{
			name: "EmptyClient",
			identity: client.MutationIdentity{
				RequestID: "request",
			},
		},
		{
			name: "EmptyRequest",
			identity: client.MutationIdentity{
				ClientID: "client",
			},
		},
		{
			name: "InvalidClientUTF8",
			identity: client.MutationIdentity{
				ClientID:  invalidUTF8,
				RequestID: "request",
			},
		},
		{
			name: "InvalidRequestUTF8",
			identity: client.MutationIdentity{
				ClientID:  "client",
				RequestID: invalidUTF8,
			},
		},
		{
			name: "BothInvalidUTF8",
			identity: client.MutationIdentity{
				ClientID:  invalidUTF8,
				RequestID: invalidUTF8,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCLIMutationIdentity(tc.identity)

			if (err == nil) != tc.valid {
				t.Fatalf(
					"identity=%+v valid=%t err=%v",
					tc.identity,
					tc.valid,
					err,
				)
			}
		})
	}
}

func TestCLIIdentityInvalidExplicitIdentity(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})

	cases := []cliConfig{
		{
			ClientID:  "",
			RequestID: "request",
		},
		{
			ClientID:  invalidUTF8,
			RequestID: "request",
		},
		{
			ClientID:  "client",
			RequestID: invalidUTF8,
		},
	}

	for _, cfg := range cases {
		kv := newCLIIdentityTestClient()

		identity, err := resolveCLIMutationIdentity(kv, cfg)

		if err == nil {
			t.Fatal("invalid explicit identity accepted")
		}

		if identity != (client.MutationIdentity{}) {
			t.Fatal("invalid identity returned to caller")
		}

		if kv.identityCalls != 0 ||
			kv.mutationCalls != 0 {
			t.Fatal("invalid explicit identity reached client")
		}
	}
}
