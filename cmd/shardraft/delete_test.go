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

type cliDeleteTestClient struct {
	cliKVClient

	identity    client.MutationIdentity
	identityErr error
	deleteErr   error
	applied     bool

	identityCalls int
	deleteCalls   int
	closeCalls    int

	lastKey      string
	lastIdentity client.MutationIdentity
	lastContext  context.Context
}

func newCLIDeleteTestClient() *cliDeleteTestClient {
	return &cliDeleteTestClient{
		identity: client.MutationIdentity{
			ClientID:  "generated-client",
			RequestID: "generated-request",
		},
		applied: true,
	}
}

func (c *cliDeleteTestClient) NewMutationIdentity() (
	client.MutationIdentity,
	error,
) {
	c.identityCalls++

	return c.identity, c.identityErr
}

func (c *cliDeleteTestClient) DeleteWithIdentity(
	ctx context.Context,
	key string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	c.deleteCalls++
	c.lastKey = key
	c.lastIdentity = identity
	c.lastContext = ctx

	if c.deleteErr != nil {
		return client.MutationResult{}, c.deleteErr
	}

	return client.MutationResult{
		Applied:  c.applied,
		Value:    "",
		Identity: identity,
	}, nil
}

func (c *cliDeleteTestClient) Close() error {
	c.closeCalls++

	return nil
}

func runCLIDeleteTest(
	kv *cliDeleteTestClient,
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

func TestCLIDeleteAutomaticIdentity(t *testing.T) {
	kv := newCLIDeleteTestClient()

	var stdout, stderr bytes.Buffer

	code := runCLIDeleteTest(
		kv,
		[]string{"delete", "balance"},
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
		Operation: cliDelete,
		Key:       "balance",
		Value:     "",
		Applied:   true,
		ClientID:  kv.identity.ClientID,
		RequestID: kv.identity.RequestID,
	}

	if result != expected {
		t.Fatalf("got %+v, want %+v", result, expected)
	}

	if kv.identityCalls != 1 ||
		kv.deleteCalls != 1 ||
		kv.closeCalls != 1 ||
		kv.lastIdentity != kv.identity ||
		kv.lastKey != "balance" {
		t.Fatal("incorrect DELETE execution")
	}
}

func TestCLIDeleteExplicitIdentity(t *testing.T) {
	kv := newCLIDeleteTestClient()

	var stdout, stderr bytes.Buffer

	code := runCLIDeleteTest(
		kv,
		[]string{
			"--client-id", "client-1",
			"--request-id", "request-1",
			"delete", "balance",
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
		kv.deleteCalls != 1 ||
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

func TestCLIDeleteClientOnlyIdentity(t *testing.T) {
	kv := newCLIDeleteTestClient()
	kv.identity.ClientID = "client-1"

	code := runCLIDeleteTest(
		kv,
		[]string{
			"--client-id", "client-1",
			"delete", "key",
		},
		io.Discard,
		io.Discard,
	)

	if code != cliExitSuccess ||
		kv.identityCalls != 1 ||
		kv.deleteCalls != 1 ||
		kv.lastIdentity != kv.identity {
		t.Fatal("incorrect client-only identity handling")
	}
}

func TestCLIDeleteNotApplied(t *testing.T) {
	kv := newCLIDeleteTestClient()
	kv.applied = false

	var stdout, stderr bytes.Buffer

	code := runCLIDeleteTest(
		kv,
		[]string{"delete", "missing-key"},
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
		result.Operation != cliDelete ||
		result.Key != "missing-key" {
		t.Fatalf("unexpected result: %+v", result)
	}

	if kv.deleteCalls != 1 || kv.closeCalls != 1 {
		t.Fatal("incorrect DELETE lifecycle")
	}
}

func TestCLIDeleteSpecialKeys(t *testing.T) {
	for _, key := range []string{
		"hello world",
		"你好",
		"ключ",
		"line1\nline2",
		"quoted \"key\"",
		"tab\tkey",
		"backslash\\key",
		"-key",
	} {
		kv := newCLIDeleteTestClient()

		var stdout bytes.Buffer

		code := runCLIDeleteTest(
			kv,
			[]string{"delete", "--", key},
			&stdout,
			io.Discard,
		)

		if code != cliExitSuccess || kv.lastKey != key {
			t.Fatalf("incorrect DELETE key: %q", key)
		}

		var result cliMutationResult

		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatal(err)
		}

		if result.Key != key || !result.Applied {
			t.Fatal("mutation result changed key")
		}
	}
}

func TestCLIDeleteTextOutput(t *testing.T) {
	for _, applied := range []bool{true, false} {
		kv := newCLIDeleteTestClient()
		kv.applied = applied

		var stdout, stderr bytes.Buffer

		code := runCLIDeleteTest(
			kv,
			[]string{
				"--output", "text",
				"delete", "balance",
			},
			&stdout,
			&stderr,
		)

		expected := "Operation: delete\n" +
			"Key: \"balance\"\n" +
			"Value: \"\"\n" +
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

func TestCLIDeleteIdentityGenerationFailure(t *testing.T) {
	kv := newCLIDeleteTestClient()
	kv.identityErr = errors.New("identity generation failed")

	var stdout, stderr bytes.Buffer

	code := runCLIDeleteTest(
		kv,
		[]string{"delete", "key"},
		&stdout,
		&stderr,
	)

	if code != cliExitFailure ||
		kv.deleteCalls != 0 ||
		kv.closeCalls != 1 ||
		stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "identity generation failed") {
		t.Fatal("incorrect identity failure handling")
	}
}

func TestCLIDeleteInvalidGeneratedIdentity(t *testing.T) {
	for _, identity := range []client.MutationIdentity{
		{},
		{ClientID: "client"},
		{RequestID: "request"},
	} {
		kv := newCLIDeleteTestClient()
		kv.identity = identity

		code := runCLIDeleteTest(
			kv,
			[]string{"delete", "key"},
			io.Discard,
			io.Discard,
		)

		if code != cliExitFailure ||
			kv.deleteCalls != 0 ||
			kv.closeCalls != 1 {
			t.Fatal("invalid identity reached backend")
		}
	}
}

func TestCLIDeleteMismatchedClientIdentity(t *testing.T) {
	kv := newCLIDeleteTestClient()

	code := runCLIDeleteTest(
		kv,
		[]string{
			"--client-id", "different-client",
			"delete", "key",
		},
		io.Discard,
		io.Discard,
	)

	if code != cliExitFailure ||
		kv.deleteCalls != 0 ||
		kv.closeCalls != 1 {
		t.Fatal("mismatched identity reached backend")
	}
}

func TestCLIDeleteClientErrors(t *testing.T) {
	for _, failure := range []error{
		errors.New("backend unavailable"),
		context.DeadlineExceeded,
		context.Canceled,
	} {
		kv := newCLIDeleteTestClient()
		kv.deleteErr = failure

		var stdout, stderr bytes.Buffer

		code := runCLIDeleteTest(
			kv,
			[]string{"delete", "key"},
			&stdout,
			&stderr,
		)

		if code != cliExitFailure ||
			kv.deleteCalls != 1 ||
			kv.closeCalls != 1 ||
			stdout.Len() != 0 ||
			!strings.Contains(stderr.String(), failure.Error()) ||
			!strings.Contains(stderr.String(), kv.identity.ClientID) ||
			!strings.Contains(stderr.String(), kv.identity.RequestID) {
			t.Fatal("incorrect DELETE error handling")
		}
	}
}

func TestCLIDeleteOutcomeUnknown(t *testing.T) {
	kv := newCLIDeleteTestClient()

	kv.deleteErr = fmt.Errorf(
		"retry failed: %w",
		client.ErrMutationOutcomeUnknown,
	)

	var stdout, stderr bytes.Buffer

	code := runCLIDeleteTest(
		kv,
		[]string{
			"--client-id", "original-client",
			"--request-id", "original-request",
			"delete", "key",
		},
		&stdout,
		&stderr,
	)

	if code != cliExitOutcomeUnknown ||
		stdout.Len() != 0 ||
		kv.deleteCalls != 1 ||
		kv.closeCalls != 1 ||
		!strings.Contains(stderr.String(), "original-client") ||
		!strings.Contains(stderr.String(), "original-request") ||
		!strings.Contains(stderr.String(), "Mutation outcome is unknown") {
		t.Fatal("incorrect unknown-outcome handling")
	}
}

func TestCLIDeleteIdentityReplay(t *testing.T) {
	args := []string{
		"--client-id", "stable-client",
		"--request-id", "stable-request",
		"delete", "key",
	}

	first := newCLIDeleteTestClient()
	second := newCLIDeleteTestClient()

	var firstOutput, secondOutput bytes.Buffer

	for _, invocation := range []struct {
		kv     *cliDeleteTestClient
		output *bytes.Buffer
	}{
		{first, &firstOutput},
		{second, &secondOutput},
	} {
		code := runCLIDeleteTest(
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
		firstOutput.String() != secondOutput.String() ||
		first.identityCalls != 0 ||
		second.identityCalls != 0 ||
		first.deleteCalls != 1 ||
		second.deleteCalls != 1 {
		t.Fatal("replay changed identity or payload")
	}
}

func TestCLIDeleteOutputFailure(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		kv := newCLIDeleteTestClient()

		var stderr bytes.Buffer

		code := runCLIDeleteTest(
			kv,
			[]string{
				"--output", format,
				"delete", "key",
			},
			cliGetFailingWriter{},
			&stderr,
		)

		if code != cliExitFailure ||
			kv.deleteCalls != 1 ||
			kv.closeCalls != 1 ||
			!strings.Contains(stderr.String(), "output failed") ||
			!strings.Contains(stderr.String(), kv.identity.ClientID) ||
			!strings.Contains(stderr.String(), kv.identity.RequestID) {
			t.Fatal("output failure lost mutation identity")
		}
	}
}

func TestCLIDeleteContextPropagation(t *testing.T) {
	kv := newCLIDeleteTestClient()

	code := runCLIDeleteTest(
		kv,
		[]string{
			"--timeout", "5s",
			"delete", "key",
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

func TestCLIDeleteInvalidArgumentsNoClient(t *testing.T) {
	for _, args := range [][]string{
		{"delete"},
		{"delete", ""},
		{"delete", "key", "extra"},
		{"delete", "--local", "key"},
		{"delete", string([]byte{0xff})},
		{"--request-id", "request", "delete", "key"},
	} {
		code := runCLI(
			context.Background(),
			args,
			io.Discard,
			io.Discard,
			func(cliConfig) (cliKVClient, error) {
				t.Fatal("invalid DELETE must not initialize client")
				return nil, nil
			},
			executeCLICommand,
		)

		if code != cliExitUsage {
			t.Fatalf("unexpected exit code: %d", code)
		}
	}
}
