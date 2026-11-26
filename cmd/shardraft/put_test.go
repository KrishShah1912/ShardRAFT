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

type cliPutTestClient struct {
	cliKVClient

	identity    client.MutationIdentity
	identityErr error
	putErr      error

	identityCalls int
	putCalls      int
	closeCalls    int

	lastKey      string
	lastValue    string
	lastIdentity client.MutationIdentity
	lastContext  context.Context
}

func newCLIPutTestClient() *cliPutTestClient {
	return &cliPutTestClient{
		identity: client.MutationIdentity{
			ClientID:  "generated-client",
			RequestID: "generated-request",
		},
	}
}

func (c *cliPutTestClient) NewMutationIdentity() (
	client.MutationIdentity,
	error,
) {
	c.identityCalls++

	return c.identity, c.identityErr
}

func (c *cliPutTestClient) PutWithIdentity(
	ctx context.Context,
	key string,
	value string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	c.putCalls++
	c.lastKey = key
	c.lastValue = value
	c.lastIdentity = identity
	c.lastContext = ctx

	if c.putErr != nil {
		return client.MutationResult{}, c.putErr
	}

	return client.MutationResult{
		Applied:  true,
		Value:    value,
		Identity: identity,
	}, nil
}

func (c *cliPutTestClient) Close() error {
	c.closeCalls++
	return nil
}

func runCLIPutTest(
	kv *cliPutTestClient,
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

func TestCLIPutAutomaticIdentity(t *testing.T) {
	kv := newCLIPutTestClient()

	var stdout, stderr bytes.Buffer

	code := runCLIPutTest(
		kv,
		[]string{"put", "balance", "100"},
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
		Operation: cliPut,
		Key:       "balance",
		Value:     "100",
		Applied:   true,
		ClientID:  kv.identity.ClientID,
		RequestID: kv.identity.RequestID,
	}

	if result != expected {
		t.Fatalf("got %+v, want %+v", result, expected)
	}

	if kv.identityCalls != 1 ||
		kv.putCalls != 1 ||
		kv.closeCalls != 1 ||
		kv.lastIdentity != kv.identity ||
		kv.lastKey != "balance" ||
		kv.lastValue != "100" {
		t.Fatal("incorrect mutation execution")
	}
}

func TestCLIPutExplicitIdentity(t *testing.T) {
	kv := newCLIPutTestClient()

	var stdout, stderr bytes.Buffer

	code := runCLIPutTest(
		kv,
		[]string{
			"--client-id", "client-1",
			"--request-id", "request-1",
			"put", "balance", "100",
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
		kv.putCalls != 1 ||
		kv.lastIdentity != expected {
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

func TestCLIPutClientOnlyIdentity(t *testing.T) {
	kv := newCLIPutTestClient()
	kv.identity.ClientID = "client-1"

	code := runCLIPutTest(
		kv,
		[]string{
			"--client-id", "client-1",
			"put", "key", "value",
		},
		io.Discard,
		io.Discard,
	)

	if code != cliExitSuccess ||
		kv.identityCalls != 1 ||
		kv.lastIdentity != kv.identity {
		t.Fatal("incorrect client-only identity handling")
	}
}

func TestCLIPutSpecialValues(t *testing.T) {
	for _, value := range []string{
		"",
		"hello world",
		"你好",
		"line1\nline2",
		"quoted \"value\"",
		"tab\tvalue",
		"backslash\\value",
		"-100",
	} {
		kv := newCLIPutTestClient()

		var stdout bytes.Buffer

		code := runCLIPutTest(
			kv,
			[]string{"put", "key", value},
			&stdout,
			io.Discard,
		)

		if code != cliExitSuccess || kv.lastValue != value {
			t.Fatalf("incorrect PUT value: %q", value)
		}

		var result cliMutationResult

		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatal(err)
		}

		if result.Value != value || !result.Applied {
			t.Fatal("mutation result changed value")
		}
	}
}

func TestCLIPutTextOutput(t *testing.T) {
	kv := newCLIPutTestClient()

	var stdout, stderr bytes.Buffer

	code := runCLIPutTest(
		kv,
		[]string{
			"--output", "text",
			"put", "balance", "100",
		},
		&stdout,
		&stderr,
	)

	expected := "Operation: put\n" +
		"Key: \"balance\"\n" +
		"Value: \"100\"\n" +
		"Applied: true\n" +
		"Client ID: \"generated-client\"\n" +
		"Request ID: \"generated-request\"\n"

	if code != cliExitSuccess ||
		stdout.String() != expected ||
		stderr.Len() != 0 {
		t.Fatalf("unexpected output: %s", stdout.String())
	}
}

func TestCLIPutIdentityGenerationFailure(t *testing.T) {
	kv := newCLIPutTestClient()
	kv.identityErr = errors.New("identity generation failed")

	var stdout, stderr bytes.Buffer

	code := runCLIPutTest(
		kv,
		[]string{"put", "key", "value"},
		&stdout,
		&stderr,
	)

	if code != cliExitFailure ||
		kv.putCalls != 0 ||
		kv.closeCalls != 1 ||
		stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "identity generation failed") {
		t.Fatal("incorrect identity failure handling")
	}
}

func TestCLIPutInvalidGeneratedIdentity(t *testing.T) {
	for _, identity := range []client.MutationIdentity{
		{},
		{ClientID: "client"},
		{RequestID: "request"},
	} {
		kv := newCLIPutTestClient()
		kv.identity = identity

		code := runCLIPutTest(
			kv,
			[]string{"put", "key", "value"},
			io.Discard,
			io.Discard,
		)

		if code != cliExitFailure ||
			kv.putCalls != 0 ||
			kv.closeCalls != 1 {
			t.Fatal("invalid identity reached backend")
		}
	}
}

func TestCLIPutMismatchedClientIdentity(t *testing.T) {
	kv := newCLIPutTestClient()

	code := runCLIPutTest(
		kv,
		[]string{
			"--client-id", "different-client",
			"put", "key", "value",
		},
		io.Discard,
		io.Discard,
	)

	if code != cliExitFailure ||
		kv.putCalls != 0 ||
		kv.closeCalls != 1 {
		t.Fatal("mismatched identity reached backend")
	}
}

func TestCLIPutClientErrors(t *testing.T) {
	for _, failure := range []error{
		errors.New("backend unavailable"),
		context.DeadlineExceeded,
		context.Canceled,
	} {
		kv := newCLIPutTestClient()
		kv.putErr = failure

		var stdout, stderr bytes.Buffer

		code := runCLIPutTest(
			kv,
			[]string{"put", "key", "value"},
			&stdout,
			&stderr,
		)

		if code != cliExitFailure ||
			kv.putCalls != 1 ||
			kv.closeCalls != 1 ||
			stdout.Len() != 0 ||
			!strings.Contains(stderr.String(), failure.Error()) ||
			!strings.Contains(stderr.String(), kv.identity.ClientID) ||
			!strings.Contains(stderr.String(), kv.identity.RequestID) {
			t.Fatal("incorrect mutation failure handling")
		}
	}
}

func TestCLIPutOutcomeUnknown(t *testing.T) {
	kv := newCLIPutTestClient()

	kv.putErr = fmt.Errorf(
		"retry failed: %w",
		client.ErrMutationOutcomeUnknown,
	)

	var stdout, stderr bytes.Buffer

	code := runCLIPutTest(
		kv,
		[]string{
			"--client-id", "original-client",
			"--request-id", "original-request",
			"put", "key", "value",
		},
		&stdout,
		&stderr,
	)

	if code != cliExitOutcomeUnknown ||
		stdout.Len() != 0 ||
		kv.putCalls != 1 ||
		kv.closeCalls != 1 ||
		!strings.Contains(stderr.String(), "original-client") ||
		!strings.Contains(stderr.String(), "original-request") ||
		!strings.Contains(stderr.String(), "Mutation outcome is unknown") {
		t.Fatal("incorrect unknown-outcome handling")
	}
}

func TestCLIPutIdentityReplay(t *testing.T) {
	args := []string{
		"--client-id", "stable-client",
		"--request-id", "stable-request",
		"put", "key", "value",
	}

	first := newCLIPutTestClient()
	second := newCLIPutTestClient()

	var firstOutput, secondOutput bytes.Buffer

	for _, invocation := range []struct {
		kv     *cliPutTestClient
		output *bytes.Buffer
	}{
		{first, &firstOutput},
		{second, &secondOutput},
	} {
		code := runCLIPutTest(
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
		first.lastValue != second.lastValue ||
		firstOutput.String() != secondOutput.String() ||
		first.identityCalls != 0 ||
		second.identityCalls != 0 {
		t.Fatal("replay changed request identity or payload")
	}
}

func TestCLIPutOutputFailure(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		kv := newCLIPutTestClient()

		var stderr bytes.Buffer

		code := runCLIPutTest(
			kv,
			[]string{
				"--output", format,
				"put", "key", "value",
			},
			cliGetFailingWriter{},
			&stderr,
		)

		if code != cliExitFailure ||
			kv.putCalls != 1 ||
			kv.closeCalls != 1 ||
			!strings.Contains(stderr.String(), "output failed") ||
			!strings.Contains(stderr.String(), kv.identity.ClientID) ||
			!strings.Contains(stderr.String(), kv.identity.RequestID) {
			t.Fatal("output failure lost mutation identity")
		}
	}
}

func TestCLIPutInvalidOutputFormat(t *testing.T) {
	err := writeCLIMutationResult(
		io.Discard,
		"xml",
		cliMutationResult{},
	)

	if err == nil {
		t.Fatal("expected invalid format rejection")
	}
}

func TestCLIPutContextPropagation(t *testing.T) {
	kv := newCLIPutTestClient()

	code := runCLIPutTest(
		kv,
		[]string{
			"--timeout", "5s",
			"put", "key", "value",
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

func TestCLIPutInvalidArgumentsNoClient(t *testing.T) {
	for _, args := range [][]string{
		{"put"},
		{"put", "key"},
		{"put", "", "value"},
		{"put", "key", "value", "extra"},
		{"--request-id", "request", "put", "key", "value"},
		{"put", "--local", "key", "value"},
	} {
		code := runCLI(
			context.Background(),
			args,
			io.Discard,
			io.Discard,
			func(cliConfig) (cliKVClient, error) {
				t.Fatal("invalid PUT must not initialize client")
				return nil, nil
			},
			executeCLICommand,
		)

		if code != cliExitUsage {
			t.Fatalf("unexpected exit code: %d", code)
		}
	}
}
