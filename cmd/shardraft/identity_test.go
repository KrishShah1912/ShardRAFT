package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/client"
)

type cliIdentityTestClient struct {
	cliKVClient

	identity    client.MutationIdentity
	identityErr error
	mutationErr error

	identityCalls int
	mutationCalls int
	closeCalls    int

	operation cliOperation
	key       string
	expected  string
	value     string
	received  client.MutationIdentity
}

func newCLIIdentityTestClient() *cliIdentityTestClient {
	return &cliIdentityTestClient{
		identity: client.MutationIdentity{
			ClientID:  "generated-client",
			RequestID: "generated-request",
		},
	}
}

func (c *cliIdentityTestClient) NewMutationIdentity() (
	client.MutationIdentity,
	error,
) {
	c.identityCalls++
	return c.identity, c.identityErr
}

func (c *cliIdentityTestClient) record(
	operation cliOperation,
	key string,
	expected string,
	value string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	c.mutationCalls++
	c.operation = operation
	c.key = key
	c.expected = expected
	c.value = value
	c.received = identity

	if c.mutationErr != nil {
		return client.MutationResult{
			Identity: identity,
		}, c.mutationErr
	}

	return client.MutationResult{
		Applied:  true,
		Value:    value,
		Identity: identity,
	}, nil
}

func (c *cliIdentityTestClient) PutWithIdentity(
	ctx context.Context,
	key string,
	value string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	return c.record(cliPut, key, "", value, identity)
}

func (c *cliIdentityTestClient) DeleteWithIdentity(
	ctx context.Context,
	key string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	return c.record(cliDelete, key, "", "", identity)
}

func (c *cliIdentityTestClient) CompareAndSwapWithIdentity(
	ctx context.Context,
	key string,
	expected string,
	value string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	return c.record(cliCAS, key, expected, value, identity)
}

func (c *cliIdentityTestClient) Close() error {
	c.closeCalls++
	return nil
}

func runCLIIdentityTest(
	kv *cliIdentityTestClient,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
) int {
	return runCLI(
		context.Background(),
		args,
		stdout,
		stderr,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		executeCLICommand,
	)
}

type cliIdentityTestCase struct {
	name      string
	operation cliOperation
	args      []string
	expected  string
	value     string
}

func cliIdentityMutationCases() []cliIdentityTestCase {
	return []cliIdentityTestCase{
		{
			name:      "Put",
			operation: cliPut,
			args:      []string{"put", "balance", "200"},
			value:     "200",
		},
		{
			name:      "Delete",
			operation: cliDelete,
			args:      []string{"delete", "balance"},
		},
		{
			name:      "CAS",
			operation: cliCAS,
			args:      []string{"cas", "balance", "100", "200"},
			expected:  "100",
			value:     "200",
		},
	}
}

func TestCLIIdentityGeneratedOncePerMutation(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIIdentityTestClient()

			var stdout, stderr bytes.Buffer

			code := runCLIIdentityTest(
				kv,
				tc.args,
				&stdout,
				&stderr,
			)

			if code != cliExitSuccess ||
				stderr.Len() != 0 ||
				kv.identityCalls != 1 ||
				kv.mutationCalls != 1 ||
				kv.closeCalls != 1 ||
				kv.received != kv.identity {
				t.Fatal("incorrect mutation identity lifecycle")
			}

			var result cliMutationResult

			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}

			if result.ClientID != kv.identity.ClientID ||
				result.RequestID != kv.identity.RequestID ||
				result.Operation != tc.operation {
				t.Fatal("response identity does not match request")
			}
		})
	}
}

func TestCLIIdentityExplicitPreservation(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIIdentityTestClient()

			identity := client.MutationIdentity{
				ClientID:  "client with spaces",
				RequestID: "request-你好",
			}

			args := append(
				[]string{
					"--client-id", identity.ClientID,
					"--request-id", identity.RequestID,
				},
				tc.args...,
			)

			var stdout bytes.Buffer

			code := runCLIIdentityTest(
				kv,
				args,
				&stdout,
				io.Discard,
			)

			if code != cliExitSuccess ||
				kv.identityCalls != 0 ||
				kv.mutationCalls != 1 ||
				kv.received != identity ||
				kv.operation != tc.operation ||
				kv.key != "balance" ||
				kv.expected != tc.expected ||
				kv.value != tc.value {
				t.Fatal("explicit identity or payload changed")
			}

			var result cliMutationResult

			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}

			if result.ClientID != identity.ClientID ||
				result.RequestID != identity.RequestID {
				t.Fatal("output identity changed")
			}
		})
	}
}

func TestCLIIdentityClientOnlyPreservation(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIIdentityTestClient()
			kv.identity.ClientID = "persistent-client"

			args := append(
				[]string{
					"--client-id", "persistent-client",
				},
				tc.args...,
			)

			code := runCLIIdentityTest(
				kv,
				args,
				io.Discard,
				io.Discard,
			)

			if code != cliExitSuccess ||
				kv.identityCalls != 1 ||
				kv.mutationCalls != 1 ||
				kv.received != kv.identity {
				t.Fatal("client-only identity was not preserved")
			}
		})
	}
}

func TestCLIIdentityGenerationFailureStopsDispatch(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIIdentityTestClient()
			kv.identityErr = errors.New("identity generation failed")

			var stdout, stderr bytes.Buffer

			code := runCLIIdentityTest(
				kv,
				tc.args,
				&stdout,
				&stderr,
			)

			if code != cliExitFailure ||
				kv.identityCalls != 1 ||
				kv.mutationCalls != 0 ||
				kv.closeCalls != 1 ||
				stdout.Len() != 0 ||
				!strings.Contains(stderr.String(), "identity generation failed") {
				t.Fatal("failed identity generation reached mutation dispatch")
			}
		})
	}
}

func TestCLIIdentityInvalidGeneratedIdentityStopsDispatch(t *testing.T) {
	identities := []client.MutationIdentity{
		{},
		{ClientID: "client"},
		{RequestID: "request"},
		{
			ClientID:  string([]byte{0xff}),
			RequestID: "request",
		},
		{
			ClientID:  "client",
			RequestID: string([]byte{0xff}),
		},
	}

	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, identity := range identities {
				kv := newCLIIdentityTestClient()
				kv.identity = identity

				code := runCLIIdentityTest(
					kv,
					tc.args,
					io.Discard,
					io.Discard,
				)

				if code != cliExitFailure ||
					kv.identityCalls != 1 ||
					kv.mutationCalls != 0 ||
					kv.closeCalls != 1 {
					t.Fatal("invalid identity reached mutation dispatch")
				}
			}
		})
	}
}

func TestCLIIdentityUnknownOutcomeDoesNotRetryAtCLILayer(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIIdentityTestClient()

			kv.mutationErr = errors.Join(
				client.ErrMutationOutcomeUnknown,
				context.DeadlineExceeded,
			)

			var stdout, stderr bytes.Buffer

			code := runCLIIdentityTest(
				kv,
				tc.args,
				&stdout,
				&stderr,
			)

			if code != cliExitOutcomeUnknown ||
				kv.identityCalls != 1 ||
				kv.mutationCalls != 1 ||
				kv.closeCalls != 1 ||
				stdout.Len() != 0 {
				t.Fatal("incorrect ambiguous mutation handling")
			}

			message := stderr.String()

			if !strings.Contains(message, kv.identity.ClientID) ||
				!strings.Contains(message, kv.identity.RequestID) ||
				!strings.Contains(message, "Mutation outcome is unknown") ||
				!strings.Contains(message, "original client-id and request-id") {
				t.Fatalf("missing recovery information: %s", message)
			}
		})
	}
}

func TestCLIIdentityOrdinaryFailureDoesNotRetryAtCLILayer(t *testing.T) {
	failures := []error{
		context.Canceled,
		context.DeadlineExceeded,
		client.ErrRetryExhausted,
		errors.New("backend unavailable"),
	}

	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, failure := range failures {
				kv := newCLIIdentityTestClient()
				kv.mutationErr = failure

				var stdout, stderr bytes.Buffer

				code := runCLIIdentityTest(
					kv,
					tc.args,
					&stdout,
					&stderr,
				)

				if code != cliExitFailure ||
					kv.identityCalls != 1 ||
					kv.mutationCalls != 1 ||
					kv.closeCalls != 1 ||
					stdout.Len() != 0 ||
					!strings.Contains(stderr.String(), kv.identity.ClientID) ||
					!strings.Contains(stderr.String(), kv.identity.RequestID) {
					t.Fatal("incorrect ordinary mutation failure handling")
				}
			}
		})
	}
}

func TestCLIIdentityRecoveryReplayPreservesPayload(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			first := newCLIIdentityTestClient()
			first.mutationErr = client.ErrMutationOutcomeUnknown

			var stderr bytes.Buffer

			code := runCLIIdentityTest(
				first,
				tc.args,
				io.Discard,
				&stderr,
			)

			if code != cliExitOutcomeUnknown ||
				first.mutationCalls != 1 {
				t.Fatal("expected unknown mutation outcome")
			}

			identity := first.received

			if !strings.Contains(stderr.String(), identity.ClientID) ||
				!strings.Contains(stderr.String(), identity.RequestID) {
				t.Fatal("recovery identity was not reported")
			}

			second := newCLIIdentityTestClient()

			args := append(
				[]string{
					"--client-id", identity.ClientID,
					"--request-id", identity.RequestID,
				},
				tc.args...,
			)

			var stdout bytes.Buffer

			code = runCLIIdentityTest(
				second,
				args,
				&stdout,
				io.Discard,
			)

			if code != cliExitSuccess ||
				second.identityCalls != 0 ||
				second.mutationCalls != 1 ||
				second.received != first.received ||
				second.operation != first.operation ||
				second.key != first.key ||
				second.expected != first.expected ||
				second.value != first.value {
				t.Fatal("recovery replay changed identity or payload")
			}

			var result cliMutationResult

			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}

			if result.ClientID != identity.ClientID ||
				result.RequestID != identity.RequestID {
				t.Fatal("recovered response changed identity")
			}
		})
	}
}

func TestCLIIdentityOutputFailurePreservesRecoveryInformation(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, format := range []string{"json", "text"} {
				kv := newCLIIdentityTestClient()

				args := append(
					[]string{"--output", format},
					tc.args...,
				)

				var stderr bytes.Buffer

				code := runCLIIdentityTest(
					kv,
					args,
					cliGetFailingWriter{},
					&stderr,
				)

				if code != cliExitFailure ||
					kv.mutationCalls != 1 ||
					kv.identityCalls != 1 ||
					kv.closeCalls != 1 ||
					!strings.Contains(stderr.String(), kv.identity.ClientID) ||
					!strings.Contains(stderr.String(), kv.identity.RequestID) ||
					!strings.Contains(stderr.String(), "same mutation identity") {
					t.Fatal("output failure lost recovery information")
				}
			}
		})
	}
}

func TestCLIIdentityRealClientSequentialUniqueness(t *testing.T) {
	cfg := defaultCLIConfig()
	cfg.ClientID = "identity-test-client"

	kv, err := newCLIClient(cfg)

	if err != nil {
		t.Fatal(err)
	}

	defer kv.Close()

	const count = 1000

	seen := make(map[string]bool, count)

	for i := 0; i < count; i++ {
		identity, err := kv.NewMutationIdentity()

		if err != nil {
			t.Fatal(err)
		}

		if err := identity.Validate(); err != nil {
			t.Fatal(err)
		}

		if identity.ClientID != cfg.ClientID {
			t.Fatal("client identity changed")
		}

		if seen[identity.RequestID] {
			t.Fatalf("duplicate request identity: %q", identity.RequestID)
		}

		seen[identity.RequestID] = true
	}

	t.Logf("generated=%d unique=%d", count, len(seen))
}

func TestCLIIdentityRealClientConcurrentUniqueness(t *testing.T) {
	cfg := defaultCLIConfig()
	cfg.ClientID = "concurrent-identity-client"

	kv, err := newCLIClient(cfg)

	if err != nil {
		t.Fatal(err)
	}

	defer kv.Close()

	const workers = 32
	const perWorker = 32
	const total = workers * perWorker

	type outcome struct {
		identity client.MutationIdentity
		err      error
	}

	results := make(chan outcome, total)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < perWorker; j++ {
				identity, err := kv.NewMutationIdentity()

				results <- outcome{
					identity: identity,
					err:      err,
				}
			}
		}()
	}

	wg.Wait()
	close(results)

	seen := make(map[string]bool, total)

	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}

		if err := result.identity.Validate(); err != nil {
			t.Fatal(err)
		}

		if result.identity.ClientID != cfg.ClientID {
			t.Fatal("client identity changed")
		}

		if seen[result.identity.RequestID] {
			t.Fatal("duplicate concurrent request identity")
		}

		seen[result.identity.RequestID] = true
	}

	if len(seen) != total {
		t.Fatalf("got %d identities, want %d", len(seen), total)
	}

	t.Logf(
		"workers=%d generated=%d unique=%d",
		workers,
		total,
		len(seen),
	)
}

func TestCLIIdentityRealClientIndependentInvocations(t *testing.T) {
	const count = 100

	clients := make(map[string]bool, count)
	requests := make(map[string]bool, count)

	for i := 0; i < count; i++ {
		cfg := defaultCLIConfig()

		kv, err := newCLIClient(cfg)

		if err != nil {
			t.Fatal(err)
		}

		identity, identityErr := kv.NewMutationIdentity()
		closeErr := kv.Close()

		if err := errors.Join(identityErr, closeErr); err != nil {
			t.Fatal(err)
		}

		if err := identity.Validate(); err != nil {
			t.Fatal(err)
		}

		if clients[identity.ClientID] ||
			requests[identity.RequestID] {
			t.Fatal("independent invocation reused generated identity")
		}

		clients[identity.ClientID] = true
		requests[identity.RequestID] = true
	}

	t.Logf(
		"invocations=%d unique_clients=%d unique_requests=%d",
		count,
		len(clients),
		len(requests),
	)
}

func TestCLIIdentityRealClientStableExplicitClient(t *testing.T) {
	const count = 100

	seen := make(map[string]bool, count)

	for i := 0; i < count; i++ {
		cfg := defaultCLIConfig()
		cfg.ClientID = "stable-client"

		kv, err := newCLIClient(cfg)

		if err != nil {
			t.Fatal(err)
		}

		identity, identityErr := kv.NewMutationIdentity()
		closeErr := kv.Close()

		if err := errors.Join(identityErr, closeErr); err != nil {
			t.Fatal(err)
		}

		if err := identity.Validate(); err != nil {
			t.Fatal(err)
		}

		if identity.ClientID != cfg.ClientID {
			t.Fatal("explicit client identity changed")
		}

		if seen[identity.RequestID] {
			t.Fatal("request identity was reused")
		}

		seen[identity.RequestID] = true
	}

	t.Logf("stable_client=true unique_requests=%d", len(seen))
}

func TestCLIIdentityWrappedUnknownOutcomeClassification(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIIdentityTestClient()

			kv.mutationErr = fmt.Errorf(
				"operation failed: %w",
				errors.Join(
					client.ErrRetryExhausted,
					client.ErrMutationOutcomeUnknown,
				),
			)

			var stderr bytes.Buffer

			code := runCLIIdentityTest(
				kv,
				tc.args,
				io.Discard,
				&stderr,
			)

			if code != cliExitOutcomeUnknown ||
				kv.mutationCalls != 1 ||
				!strings.Contains(stderr.String(), "Mutation outcome is unknown") {
				t.Fatal("wrapped ambiguous outcome was not preserved")
			}
		})
	}
}
