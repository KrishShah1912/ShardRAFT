package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/client"
)

type cliOutputTestClient struct {
	cliIdentityTestClient

	readValue  string
	readFound  bool
	readErr    error
	readCalls  int
	localCalls int
	notApplied bool
}

func newCLIOutputTestClient() *cliOutputTestClient {
	return &cliOutputTestClient{
		cliIdentityTestClient: *newCLIIdentityTestClient(),
		readValue:             "200",
		readFound:             true,
	}
}

func (c *cliOutputTestClient) Get(
	ctx context.Context,
	key string,
) (string, bool, error) {
	c.readCalls++
	return c.readValue, c.readFound, c.readErr
}

func (c *cliOutputTestClient) GetLocal(
	ctx context.Context,
	key string,
) (string, bool, error) {
	c.localCalls++
	return c.readValue, c.readFound, c.readErr
}

func (c *cliOutputTestClient) CompareAndSwapWithIdentity(
	ctx context.Context,
	key string,
	expected string,
	value string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	result, err := c.record(
		cliCAS,
		key,
		expected,
		value,
		identity,
	)

	if c.notApplied {
		result.Applied = false
		result.Value = ""
	}

	return result, err
}

func (c *cliOutputTestClient) DeleteWithIdentity(
	ctx context.Context,
	key string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	result, err := c.record(
		cliDelete,
		key,
		"",
		"",
		identity,
	)

	if c.notApplied {
		result.Applied = false
	}

	return result, err
}

func runCLIOutputTest(
	kv *cliOutputTestClient,
	args ...string,
) (int, string, string) {
	var stdout, stderr bytes.Buffer

	code := runCLI(
		context.Background(),
		args,
		&stdout,
		&stderr,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		executeCLICommand,
	)

	return code, stdout.String(), stderr.String()
}

func decodeCLIOutputObject(
	t *testing.T,
	output string,
) map[string]json.RawMessage {
	t.Helper()

	decoder := json.NewDecoder(strings.NewReader(output))

	var object map[string]json.RawMessage

	if err := decoder.Decode(&object); err != nil {
		t.Fatalf("invalid JSON output: %v; output=%q", err, output)
	}

	if object == nil {
		t.Fatal("expected JSON object")
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected additional output: %v", err)
	}

	if !strings.HasSuffix(output, "\n") {
		t.Fatal("JSON output must end with a newline")
	}

	return object
}

func requireCLIOutputFields(
	t *testing.T,
	object map[string]json.RawMessage,
	fields ...string,
) {
	t.Helper()

	if len(object) != len(fields) {
		t.Fatalf(
			"unexpected field count: got=%d want=%d object=%v",
			len(object),
			len(fields),
			object,
		)
	}

	for _, field := range fields {
		if _, ok := object[field]; !ok {
			t.Fatalf("missing output field %q", field)
		}
	}
}

func requireCLIOutputValue(
	t *testing.T,
	object map[string]json.RawMessage,
	field string,
	expected any,
) {
	t.Helper()

	raw, ok := object[field]

	if !ok {
		t.Fatalf("missing output field %q", field)
	}

	var actual any

	if err := json.Unmarshal(raw, &actual); err != nil {
		t.Fatal(err)
	}

	if actual != expected {
		t.Fatalf(
			"field=%s got=%#v want=%#v",
			field,
			actual,
			expected,
		)
	}
}

func TestCLIOutputExitCodeContract(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		prepare  func(*cliOutputTestClient)
		expected int
	}{
		{
			name:     "Help",
			args:     []string{"--help"},
			expected: 0,
		},
		{
			name:     "SuccessfulGet",
			args:     []string{"get", "balance"},
			expected: 0,
		},
		{
			name:     "SuccessfulPut",
			args:     []string{"put", "balance", "200"},
			expected: 0,
		},
		{
			name:     "SuccessfulDelete",
			args:     []string{"delete", "balance"},
			expected: 0,
		},
		{
			name:     "SuccessfulCAS",
			args:     []string{"cas", "balance", "100", "200"},
			expected: 0,
		},
		{
			name: "ExecutionFailure",
			args: []string{"get", "balance"},
			prepare: func(kv *cliOutputTestClient) {
				kv.readErr = errors.New("backend failure")
			},
			expected: 1,
		},
		{
			name:     "InvalidArguments",
			args:     []string{"put", "balance"},
			expected: 2,
		},
		{
			name:     "UnknownCommand",
			args:     []string{"unknown"},
			expected: 2,
		},
		{
			name: "MissingKey",
			args: []string{"get", "missing"},
			prepare: func(kv *cliOutputTestClient) {
				kv.readFound = false
				kv.readValue = ""
			},
			expected: 3,
		},
		{
			name: "UnknownMutationOutcome",
			args: []string{"put", "balance", "200"},
			prepare: func(kv *cliOutputTestClient) {
				kv.mutationErr = client.ErrMutationOutcomeUnknown
			},
			expected: 4,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIOutputTestClient()

			if tc.prepare != nil {
				tc.prepare(kv)
			}

			code, stdout, stderr := runCLIOutputTest(
				kv,
				tc.args...,
			)

			if code != tc.expected {
				t.Fatalf(
					"exit=%d want=%d stdout=%q stderr=%q",
					code,
					tc.expected,
					stdout,
					stderr,
				)
			}
		})
	}
}

func TestCLIOutputGetJSONContract(t *testing.T) {
	for _, local := range []bool{false, true} {
		name := "Linearizable"
		mode := "linearizable"
		args := []string{"get", "balance"}

		if local {
			name = "Local"
			mode = "local"
			args = []string{"get", "--local", "balance"}
		}

		t.Run(name, func(t *testing.T) {
			kv := newCLIOutputTestClient()

			code, stdout, stderr := runCLIOutputTest(
				kv,
				args...,
			)

			if code != 0 || stderr != "" {
				t.Fatalf("exit=%d stderr=%q", code, stderr)
			}

			object := decodeCLIOutputObject(t, stdout)

			requireCLIOutputFields(
				t,
				object,
				"key",
				"value",
				"found",
				"read_mode",
			)

			requireCLIOutputValue(t, object, "key", "balance")
			requireCLIOutputValue(t, object, "value", "200")
			requireCLIOutputValue(t, object, "found", true)
			requireCLIOutputValue(t, object, "read_mode", mode)
		})
	}
}

func TestCLIOutputMissingAndEmptyValues(t *testing.T) {
	for _, found := range []bool{true, false} {
		kv := newCLIOutputTestClient()
		kv.readFound = found
		kv.readValue = ""

		code, stdout, stderr := runCLIOutputTest(
			kv,
			"get",
			"balance",
		)

		expected := 0

		if !found {
			expected = 3
		}

		if code != expected || stderr != "" {
			t.Fatalf(
				"found=%t exit=%d stderr=%q",
				found,
				code,
				stderr,
			)
		}

		object := decodeCLIOutputObject(t, stdout)

		requireCLIOutputValue(t, object, "found", found)
		requireCLIOutputValue(t, object, "value", "")
	}
}

func TestCLIOutputMutationJSONContract(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIOutputTestClient()

			code, stdout, stderr := runCLIOutputTest(
				kv,
				tc.args...,
			)

			if code != 0 || stderr != "" {
				t.Fatalf("exit=%d stderr=%q", code, stderr)
			}

			object := decodeCLIOutputObject(t, stdout)

			requireCLIOutputFields(
				t,
				object,
				"operation",
				"key",
				"value",
				"applied",
				"client_id",
				"request_id",
			)

			requireCLIOutputValue(
				t, object, "operation", string(tc.operation),
			)

			requireCLIOutputValue(t, object, "key", "balance")
			requireCLIOutputValue(t, object, "value", tc.value)
			requireCLIOutputValue(t, object, "applied", true)

			requireCLIOutputValue(
				t, object, "client_id", kv.identity.ClientID,
			)

			requireCLIOutputValue(
				t, object, "request_id", kv.identity.RequestID,
			)
		})
	}
}

func TestCLIOutputCASMismatchIsSuccessful(t *testing.T) {
	kv := newCLIOutputTestClient()
	kv.notApplied = true

	code, stdout, stderr := runCLIOutputTest(
		kv,
		"cas", "balance", "999", "200",
	)

	if code != 0 || stderr != "" {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}

	object := decodeCLIOutputObject(t, stdout)

	requireCLIOutputValue(t, object, "operation", "cas")
	requireCLIOutputValue(t, object, "applied", false)
}

func TestCLIOutputDeleteNotAppliedIsSuccessful(t *testing.T) {
	kv := newCLIOutputTestClient()
	kv.notApplied = true

	code, stdout, stderr := runCLIOutputTest(
		kv,
		"delete", "missing",
	)

	if code != 0 || stderr != "" {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}

	object := decodeCLIOutputObject(t, stdout)

	requireCLIOutputValue(t, object, "operation", "delete")
	requireCLIOutputValue(t, object, "applied", false)
}

func TestCLIOutputJSONSpecialCharacterRoundTrip(t *testing.T) {
	values := []string{
		"",
		"hello world",
		"你好世界",
		"line1\nline2",
		"tab\tvalue",
		"quote\"value",
		"backslash\\value",
		"<tag>&value",
		"null\x00byte",
	}

	for _, value := range values {
		kv := newCLIOutputTestClient()
		kv.readValue = value

		code, stdout, stderr := runCLIOutputTest(
			kv,
			"get", "balance",
		)

		if code != 0 || stderr != "" {
			t.Fatalf("exit=%d stderr=%q", code, stderr)
		}

		object := decodeCLIOutputObject(t, stdout)

		requireCLIOutputValue(t, object, "value", value)

		code, stdout, stderr = runCLIOutputTest(
			kv,
			"put", "balance", value,
		)

		if code != 0 || stderr != "" {
			t.Fatalf("exit=%d stderr=%q", code, stderr)
		}

		object = decodeCLIOutputObject(t, stdout)

		requireCLIOutputValue(t, object, "value", value)
	}
}

func TestCLIOutputExplicitIdentityRoundTrip(t *testing.T) {
	clientID := "client \"你好\""
	requestID := "request\\with\nnewline"

	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIOutputTestClient()

			args := append(
				[]string{
					"--client-id", clientID,
					"--request-id", requestID,
				},
				tc.args...,
			)

			code, stdout, stderr := runCLIOutputTest(
				kv,
				args...,
			)

			if code != 0 || stderr != "" {
				t.Fatalf("exit=%d stderr=%q", code, stderr)
			}

			object := decodeCLIOutputObject(t, stdout)

			requireCLIOutputValue(t, object, "client_id", clientID)
			requireCLIOutputValue(t, object, "request_id", requestID)
		})
	}
}

func TestCLIOutputTextContract(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			kv := newCLIOutputTestClient()

			args := append(
				[]string{"--output", "text"},
				tc.args...,
			)

			code, stdout, stderr := runCLIOutputTest(
				kv,
				args...,
			)

			if code != 0 || stderr != "" {
				t.Fatalf("exit=%d stderr=%q", code, stderr)
			}

			expected := []string{
				"Operation: " + string(tc.operation),
				"Key: \"balance\"",
				"Applied: true",
				"Client ID: \"generated-client\"",
				"Request ID: \"generated-request\"",
			}

			for _, fragment := range expected {
				if !strings.Contains(stdout, fragment) {
					t.Fatalf(
						"missing %q in %q",
						fragment,
						stdout,
					)
				}
			}

			if !strings.HasSuffix(stdout, "\n") {
				t.Fatal("text output missing final newline")
			}
		})
	}
}

func TestCLIOutputHelpUsesStdout(t *testing.T) {
	cases := [][]string{
		{"--help"},
		{"get", "--help"},
		{"put", "--help"},
		{"delete", "--help"},
		{"cas", "--help"},
	}

	for _, args := range cases {
		kv := newCLIOutputTestClient()

		code, stdout, stderr := runCLIOutputTest(kv, args...)

		if code != 0 ||
			stdout == "" ||
			stderr != "" ||
			kv.closeCalls != 0 ||
			kv.mutationCalls != 0 ||
			kv.readCalls != 0 {
			t.Fatalf("incorrect help output: args=%v", args)
		}
	}
}

func TestCLIOutputUsageErrorsUseStderr(t *testing.T) {
	cases := [][]string{
		{},
		{"unknown"},
		{"get"},
		{"put", "balance"},
		{"delete"},
		{"cas", "balance", "100"},
		{"--output", "xml", "get", "balance"},
		{"--timeout", "0s", "get", "balance"},
	}

	for _, args := range cases {
		kv := newCLIOutputTestClient()

		code, stdout, stderr := runCLIOutputTest(kv, args...)

		if code != 2 ||
			stdout != "" ||
			stderr == "" ||
			kv.closeCalls != 0 ||
			kv.mutationCalls != 0 ||
			kv.readCalls != 0 {
			t.Fatalf(
				"args=%v exit=%d stdout=%q stderr=%q",
				args,
				code,
				stdout,
				stderr,
			)
		}
	}
}

func TestCLIOutputReadErrorsUseStderr(t *testing.T) {
	failures := []error{
		context.Canceled,
		context.DeadlineExceeded,
		client.ErrRetryExhausted,
		errors.New("backend unavailable"),
	}

	for _, failure := range failures {
		for _, format := range []string{"json", "text"} {
			kv := newCLIOutputTestClient()
			kv.readErr = failure

			code, stdout, stderr := runCLIOutputTest(
				kv,
				"--output", format,
				"get", "balance",
			)

			if code != 1 ||
				stdout != "" ||
				stderr == "" ||
				kv.readCalls != 1 {
				t.Fatalf("incorrect read error output: %v", failure)
			}
		}
	}
}

func TestCLIOutputMutationErrorsUseStderr(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, format := range []string{"json", "text"} {
				kv := newCLIOutputTestClient()
				kv.mutationErr = errors.New("backend unavailable")

				args := append(
					[]string{"--output", format},
					tc.args...,
				)

				code, stdout, stderr := runCLIOutputTest(
					kv,
					args...,
				)

				if code != 1 ||
					stdout != "" ||
					!strings.Contains(stderr, kv.identity.ClientID) ||
					!strings.Contains(stderr, kv.identity.RequestID) ||
					kv.mutationCalls != 1 {
					t.Fatal("incorrect mutation error output")
				}
			}
		})
	}
}

func TestCLIOutputUnknownOutcomeContract(t *testing.T) {
	for _, tc := range cliIdentityMutationCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, format := range []string{"json", "text"} {
				kv := newCLIOutputTestClient()

				kv.mutationErr = errors.Join(
					context.DeadlineExceeded,
					client.ErrMutationOutcomeUnknown,
				)

				args := append(
					[]string{"--output", format},
					tc.args...,
				)

				code, stdout, stderr := runCLIOutputTest(
					kv,
					args...,
				)

				if code != 4 ||
					stdout != "" ||
					!strings.Contains(stderr, "Mutation outcome is unknown") ||
					!strings.Contains(stderr, kv.identity.ClientID) ||
					!strings.Contains(stderr, kv.identity.RequestID) ||
					kv.mutationCalls != 1 {
					t.Fatal("incorrect unknown-outcome contract")
				}
			}
		})
	}
}

func TestCLIOutputDefaultIsJSON(t *testing.T) {
	kv := newCLIOutputTestClient()

	code, stdout, stderr := runCLIOutputTest(
		kv,
		"get", "balance",
	)

	if code != 0 || stderr != "" {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}

	object := decodeCLIOutputObject(t, stdout)

	requireCLIOutputValue(t, object, "value", "200")
}
