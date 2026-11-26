package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/client"
)

type cliCASTestClient struct {
	cliKVClient

	identity    client.MutationIdentity
	identityErr error
	casErr      error
	applied     bool

	identityCalls int
	casCalls      int
	closeCalls    int

	lastKey      string
	lastExpected string
	lastValue    string
	lastIdentity client.MutationIdentity
	lastContext  context.Context
}

func newCLICASTestClient() *cliCASTestClient {
	return &cliCASTestClient{
		identity: client.MutationIdentity{
			ClientID:  "generated-client",
			RequestID: "generated-request",
		},
		applied: true,
	}
}

func (c *cliCASTestClient) NewMutationIdentity() (
	client.MutationIdentity,
	error,
) {
	c.identityCalls++

	return c.identity, c.identityErr
}

func (c *cliCASTestClient) CompareAndSwapWithIdentity(
	ctx context.Context,
	key string,
	expected string,
	value string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	c.casCalls++
	c.lastKey = key
	c.lastExpected = expected
	c.lastValue = value
	c.lastIdentity = identity
	c.lastContext = ctx

	if c.casErr != nil {
		return client.MutationResult{}, c.casErr
	}

	result := client.MutationResult{
		Applied:  c.applied,
		Identity: identity,
	}

	if c.applied {
		result.Value = value
	}

	return result, nil
}

func (c *cliCASTestClient) Close() error {
	c.closeCalls++

	return nil
}

func runCLICASTest(
	kv *cliCASTestClient,
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

func TestCLICASAutomaticIdentity(t *testing.T) {
	kv := newCLICASTestClient()

	var stdout, stderr bytes.Buffer

	code := runCLICASTest(
		kv,
		[]string{"cas", "balance", "100", "200"},
		&stdout,
		&stderr,
	)

	if code != cliExitSuccess || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr.String())
	}

	var result cliMutationResult

	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	expected := cliMutationResult{
		Operation: cliCAS,
		Key:       "balance",
		Value:     "200",
		Applied:   true,
		ClientID:  kv.identity.ClientID,
		RequestID: kv.identity.RequestID,
	}

	if result != expected {
		t.Fatalf("got %+v, want %+v", result, expected)
	}

	if kv.identityCalls != 1 ||
		kv.casCalls != 1 ||
		kv.closeCalls != 1 ||
		kv.lastIdentity != kv.identity ||
		kv.lastKey != "balance" ||
		kv.lastExpected != "100" ||
		kv.lastValue != "200" {
		t.Fatal("incorrect CAS execution")
	}
}

func TestCLICASExplicitIdentity(t *testing.T) {
	kv := newCLICASTestClient()

	var stdout, stderr bytes.Buffer

	code := runCLICASTest(
		kv,
		[]string{
			"--client-id", "client-1",
			"--request-id", "request-1",
			"cas", "balance", "100", "200",
		},
		&stdout,
		&stderr,
	)

	if code != cliExitSuccess || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr.String())
	}

	expected := client.MutationIdentity{
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	if kv.identityCalls != 0 ||
		kv.casCalls != 1 ||
		kv.lastIdentity != expected ||
		kv.closeCalls != 1 {
		t.Fatal("explicit identity was not preserved")
	}

	var result cliMutationResult

	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if result.ClientID != expected.ClientID ||
		result.RequestID != expected.RequestID {
		t.Fatal("incorrect response identity")
	}
}

func TestCLICASClientOnlyIdentity(t *testing.T) {
	kv := newCLICASTestClient()
	kv.identity.ClientID = "client-1"

	code := runCLICASTest(
		kv,
		[]string{
			"--client-id", "client-1",
			"cas", "key", "old", "new",
		},
		io.Discard,
		io.Discard,
	)

	if code != cliExitSuccess ||
		kv.identityCalls != 1 ||
		kv.casCalls != 1 ||
		kv.lastIdentity != kv.identity {
		t.Fatal("incorrect client-only identity handling")
	}
}

func TestCLICASComparisonMismatch(t *testing.T) {
	kv := newCLICASTestClient()
	kv.applied = false

	var stdout, stderr bytes.Buffer

	code := runCLICASTest(
		kv,
		[]string{"cas", "balance", "999", "200"},
		&stdout,
		&stderr,
	)

	if code != cliExitSuccess || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr.String())
	}

	var result cliMutationResult

	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if result.Applied ||
		result.Value != "" ||
		result.Operation != cliCAS ||
		result.Key != "balance" ||
		result.ClientID != kv.identity.ClientID ||
		result.RequestID != kv.identity.RequestID {
		t.Fatalf("unexpected mismatch result: %+v", result)
	}

	if kv.casCalls != 1 || kv.closeCalls != 1 {
		t.Fatal("comparison mismatch must not trigger another CLI call")
	}
}

func TestCLICASEmptyValues(t *testing.T) {
	cases := []struct {
		expected string
		value    string
	}{
		{"", "new"},
		{"old", ""},
		{"", ""},
	}

	for _, tc := range cases {
		kv := newCLICASTestClient()

		var stdout bytes.Buffer

		code := runCLICASTest(
			kv,
			[]string{"cas", "key", tc.expected, tc.value},
			&stdout,
			io.Discard,
		)

		if code != cliExitSuccess ||
			kv.lastExpected != tc.expected ||
			kv.lastValue != tc.value {
			t.Fatal("empty CAS values were not preserved")
		}

		var result cliMutationResult

		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatal(err)
		}

		if !result.Applied || result.Value != tc.value {
			t.Fatal("incorrect empty-value response")
		}
	}
}

func TestCLICASSpecialValues(t *testing.T) {
	values := []string{
		"hello world",
		"你好",
		"line1\nline2",
		"quoted \"value\"",
		"tab\tvalue",
		"backslash\\value",
		"-100",
	}

	for _, value := range values {
		kv := newCLICASTestClient()

		var stdout bytes.Buffer

		expected := "old:" + value

		code := runCLICASTest(
			kv,
			[]string{"cas", "--", "-key", expected, value},
			&stdout,
			io.Discard,
		)

		if code != cliExitSuccess ||
			kv.lastKey != "-key" ||
			kv.lastExpected != expected ||
			kv.lastValue != value {
			t.Fatal("special CAS arguments were changed")
		}

		var result cliMutationResult

		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatal(err)
		}

		if result.Value != value || !result.Applied {
			t.Fatal("CAS response changed value")
		}
	}
}

func TestCLICASTextOutput(t *testing.T) {
	for _, applied := range []bool{true, false} {
		kv := newCLICASTestClient()
		kv.applied = applied

		var stdout, stderr bytes.Buffer

		code := runCLICASTest(
			kv,
			[]string{
				"--output", "text",
				"cas", "balance", "100", "200",
			},
			&stdout,
			&stderr,
		)

		value := ""

		if applied {
			value = "200"
		}

		expected := "Operation: cas\n" +
			"Key: \"balance\"\n" +
			fmt.Sprintf("Value: %q\n", value) +
			fmt.Sprintf("Applied: %t\n", applied) +
			"Client ID: \"generated-client\"\n" +
			"Request ID: \"generated-request\"\n"

		if code != cliExitSuccess ||
			stdout.String() != expected ||
			stderr.Len() != 0 {
			t.Fatalf("unexpected output: %s", stdout.String())
		}
	}
}

func TestCLICASIdentityGenerationFailure(t *testing.T) {
	kv := newCLICASTestClient()
	kv.identityErr = errors.New("identity generation failed")

	var stdout, stderr bytes.Buffer

	code := runCLICASTest(
		kv,
		[]string{"cas", "key", "old", "new"},
		&stdout,
		&stderr,
	)

	if code != cliExitFailure ||
		kv.casCalls != 0 ||
		kv.closeCalls != 1 ||
		stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "identity generation failed") {
		t.Fatal("incorrect identity failure handling")
	}
}

func TestCLICASInvalidGeneratedIdentity(t *testing.T) {
	for _, identity := range []client.MutationIdentity{
		{},
		{ClientID: "client"},
		{RequestID: "request"},
	} {
		kv := newCLICASTestClient()
		kv.identity = identity

		code := runCLICASTest(
			kv,
			[]string{"cas", "key", "old", "new"},
			io.Discard,
			io.Discard,
		)

		if code != cliExitFailure ||
			kv.casCalls != 0 ||
			kv.closeCalls != 1 {
			t.Fatal("invalid identity reached backend")
		}
	}
}

func TestCLICASMismatchedClientIdentity(t *testing.T) {
	kv := newCLICASTestClient()

	code := runCLICASTest(
		kv,
		[]string{
			"--client-id", "different-client",
			"cas", "key", "old", "new",
		},
		io.Discard,
		io.Discard,
	)

	if code != cliExitFailure ||
		kv.casCalls != 0 ||
		kv.closeCalls != 1 {
		t.Fatal("mismatched identity reached backend")
	}
}

func TestCLICASClientErrors(t *testing.T) {
	for _, failure := range []error{
		errors.New("backend unavailable"),
		context.DeadlineExceeded,
		context.Canceled,
	} {
		kv := newCLICASTestClient()
		kv.casErr = failure

		var stdout, stderr bytes.Buffer

		code := runCLICASTest(
			kv,
			[]string{"cas", "key", "old", "new"},
			&stdout,
			&stderr,
		)

		if code != cliExitFailure ||
			kv.casCalls != 1 ||
			kv.closeCalls != 1 ||
			stdout.Len() != 0 ||
			!strings.Contains(stderr.String(), failure.Error()) ||
			!strings.Contains(stderr.String(), kv.identity.ClientID) ||
			!strings.Contains(stderr.String(), kv.identity.RequestID) {
			t.Fatal("incorrect CAS error handling")
		}
	}
}

func TestCLICASOutcomeUnknown(t *testing.T) {
	kv := newCLICASTestClient()

	kv.casErr = fmt.Errorf(
		"retry failed: %w",
		client.ErrMutationOutcomeUnknown,
	)

	var stdout, stderr bytes.Buffer

	code := runCLICASTest(
		kv,
		[]string{
			"--client-id", "original-client",
			"--request-id", "original-request",
			"cas", "key", "old", "new",
		},
		&stdout,
		&stderr,
	)

	if code != cliExitOutcomeUnknown ||
		stdout.Len() != 0 ||
		kv.casCalls != 1 ||
		kv.closeCalls != 1 ||
		!strings.Contains(stderr.String(), "original-client") ||
		!strings.Contains(stderr.String(), "original-request") ||
		!strings.Contains(stderr.String(), "Mutation outcome is unknown") {
		t.Fatal("incorrect unknown-outcome handling")
	}
}

func TestCLICASIdentityReplay(t *testing.T) {
	for _, applied := range []bool{true, false} {
		args := []string{
			"--client-id", "stable-client",
			"--request-id", "stable-request",
			"cas", "key", "old", "new",
		}

		first := newCLICASTestClient()
		second := newCLICASTestClient()

		first.applied = applied
		second.applied = applied

		var firstOutput, secondOutput bytes.Buffer

		for _, invocation := range []struct {
			kv     *cliCASTestClient
			output *bytes.Buffer
		}{
			{first, &firstOutput},
			{second, &secondOutput},
		} {
			code := runCLICASTest(
				invocation.kv,
				args,
				invocation.output,
				io.Discard,
			)

			if code != cliExitSuccess {
				t.Fatal("replay execution failed")
			}
		}

		if first.lastIdentity != second.lastIdentity ||
			first.lastKey != second.lastKey ||
			first.lastExpected != second.lastExpected ||
			first.lastValue != second.lastValue ||
			firstOutput.String() != secondOutput.String() ||
			first.identityCalls != 0 ||
			second.identityCalls != 0 ||
			first.casCalls != 1 ||
			second.casCalls != 1 {
			t.Fatal("replay changed identity or CAS payload")
		}
	}
}

func TestCLICASOutputFailure(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		for _, applied := range []bool{true, false} {
			kv := newCLICASTestClient()
			kv.applied = applied

			var stderr bytes.Buffer

			code := runCLICASTest(
				kv,
				[]string{
					"--output", format,
					"cas", "key", "old", "new",
				},
				cliGetFailingWriter{},
				&stderr,
			)

			if code != cliExitFailure ||
				kv.casCalls != 1 ||
				kv.closeCalls != 1 ||
				!strings.Contains(stderr.String(), "output failed") ||
				!strings.Contains(stderr.String(), kv.identity.ClientID) ||
				!strings.Contains(stderr.String(), kv.identity.RequestID) {
				t.Fatal("output failure lost mutation identity")
			}
		}
	}
}

func TestCLICASContextPropagation(t *testing.T) {
	kv := newCLICASTestClient()

	code := runCLICASTest(
		kv,
		[]string{
			"--timeout", "5s",
			"cas", "key", "old", "new",
		},
		io.Discard,
		io.Discard,
	)

	if code != cliExitSuccess || kv.lastContext == nil {
		t.Fatal("missing operation context")
	}

	deadline, ok := kv.lastContext.Deadline()

	if !ok || time.Until(deadline) > 5*time.Second {
		t.Fatal("incorrect operation deadline")
	}

	if !errors.Is(kv.lastContext.Err(), context.Canceled) {
		t.Fatal("operation context was not cleaned up")
	}
}

func TestCLICASInvalidArgumentsNoClient(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})

	for _, args := range [][]string{
		{"cas"},
		{"cas", "key"},
		{"cas", "key", "old"},
		{"cas", "", "old", "new"},
		{"cas", "key", "old", "new", "extra"},
		{"cas", "--local", "key", "old", "new"},
		{"cas", invalidUTF8, "old", "new"},
		{"cas", "key", invalidUTF8, "new"},
		{"cas", "key", "old", invalidUTF8},
		{"--request-id", "request", "cas", "key", "old", "new"},
	} {
		code := runCLI(
			context.Background(),
			args,
			io.Discard,
			io.Discard,
			func(cliConfig) (cliKVClient, error) {
				t.Fatal("invalid CAS must not initialize client")
				return nil, nil
			},
			executeCLICommand,
		)

		if code != cliExitUsage {
			t.Fatalf("unexpected exit code: %d", code)
		}
	}
}
