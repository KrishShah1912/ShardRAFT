package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type cliGetTestClient struct {
	cliKVClient

	value string
	found bool
	err   error

	getCalls      int
	getLocalCalls int
	closeCalls    int

	lastKey     string
	lastContext context.Context
}

func (c *cliGetTestClient) Get(
	ctx context.Context,
	key string,
) (string, bool, error) {
	c.getCalls++
	c.lastKey = key
	c.lastContext = ctx

	return c.value, c.found, c.err
}

func (c *cliGetTestClient) GetLocal(
	ctx context.Context,
	key string,
) (string, bool, error) {
	c.getLocalCalls++
	c.lastKey = key
	c.lastContext = ctx

	return c.value, c.found, c.err
}

func (c *cliGetTestClient) Close() error {
	c.closeCalls++
	return nil
}

type cliGetFailingWriter struct{}

func (cliGetFailingWriter) Write(
	_ []byte,
) (int, error) {
	return 0, errors.New("write failed")
}

func TestCLIGetReadModes(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		mode       string
		getCalls   int
		localCalls int
	}{
		{
			name:     "Linearizable",
			args:     []string{"get", "balance"},
			mode:     "linearizable",
			getCalls: 1,
		},
		{
			name:       "Local",
			args:       []string{"get", "--local", "balance"},
			mode:       "local",
			localCalls: 1,
		},
		{
			name:     "ExplicitLinearizable",
			args:     []string{"get", "--local=false", "balance"},
			mode:     "linearizable",
			getCalls: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := &cliGetTestClient{
				value: "100",
				found: true,
			}

			var stdout, stderr bytes.Buffer

			code := runCLI(
				context.Background(),
				tc.args,
				&stdout,
				&stderr,
				func(cliConfig) (cliKVClient, error) {
					return kv, nil
				},
				executeCLICommand,
			)

			if code != cliExitSuccess {
				t.Fatalf("exit=%d stderr=%s", code, stderr.String())
			}

			if stderr.Len() != 0 {
				t.Fatalf("unexpected stderr: %s", stderr.String())
			}

			var result cliGetResult

			if err := json.Unmarshal(
				stdout.Bytes(),
				&result,
			); err != nil {
				t.Fatal(err)
			}

			expected := cliGetResult{
				Key:      "balance",
				Value:    "100",
				Found:    true,
				ReadMode: tc.mode,
			}

			if result != expected {
				t.Fatalf("got %+v, want %+v", result, expected)
			}

			if kv.getCalls != tc.getCalls ||
				kv.getLocalCalls != tc.localCalls ||
				kv.closeCalls != 1 ||
				kv.lastKey != "balance" {
				t.Fatal("incorrect client invocation")
			}
		})
	}
}

func TestCLIGetMissingKey(t *testing.T) {
	for _, local := range []bool{false, true} {
		cfg := defaultCLIConfig()

		kv := &cliGetTestClient{
			value: "ignored",
			found: false,
		}

		var stdout, stderr bytes.Buffer

		code := executeCLICommand(
			context.Background(),
			kv,
			cliInvocation{
				Config: cfg,
				Command: cliCommand{
					Operation: cliGet,
					Key:       "missing",
					Local:     local,
				},
			},
			&stdout,
			&stderr,
		)

		if code != cliExitNotFound {
			t.Fatalf("unexpected exit code: %d", code)
		}

		var result cliGetResult

		if err := json.Unmarshal(
			stdout.Bytes(),
			&result,
		); err != nil {
			t.Fatal(err)
		}

		if result.Found ||
			result.Value != "" ||
			result.Key != "missing" ||
			stderr.Len() != 0 {
			t.Fatalf("unexpected missing result: %+v", result)
		}

		expectedMode := "linearizable"

		if local {
			expectedMode = "local"
		}

		if result.ReadMode != expectedMode {
			t.Fatal("incorrect read mode")
		}
	}
}

func TestCLIGetEmptyValue(t *testing.T) {
	kv := &cliGetTestClient{
		value: "",
		found: true,
	}

	var stdout bytes.Buffer

	code := runCLI(
		context.Background(),
		[]string{"get", "empty-value"},
		&stdout,
		io.Discard,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		executeCLICommand,
	)

	if code != cliExitSuccess {
		t.Fatalf("unexpected exit code: %d", code)
	}

	var result cliGetResult

	if err := json.Unmarshal(
		stdout.Bytes(),
		&result,
	); err != nil {
		t.Fatal(err)
	}

	if !result.Found || result.Value != "" {
		t.Fatal("empty value must not be treated as missing")
	}
}

func TestCLIGetSpecialValues(t *testing.T) {
	values := []string{
		"hello world",
		"你好",
		"line1\nline2",
		"quoted \"value\"",
		"tab\tvalue",
		"backslash\\value",
	}

	for _, value := range values {
		kv := &cliGetTestClient{
			value: value,
			found: true,
		}

		var stdout bytes.Buffer

		code := runCLI(
			context.Background(),
			[]string{"get", "special-key"},
			&stdout,
			io.Discard,
			func(cliConfig) (cliKVClient, error) {
				return kv, nil
			},
			executeCLICommand,
		)

		if code != cliExitSuccess {
			t.Fatalf("unexpected exit code: %d", code)
		}

		var result cliGetResult

		if err := json.Unmarshal(
			stdout.Bytes(),
			&result,
		); err != nil {
			t.Fatal(err)
		}

		if result.Value != value {
			t.Fatalf("value changed: %q != %q", result.Value, value)
		}
	}
}

func TestCLIGetTextOutput(t *testing.T) {
	kv := &cliGetTestClient{
		value: "100",
		found: true,
	}

	var stdout, stderr bytes.Buffer

	code := runCLI(
		context.Background(),
		[]string{
			"--output", "text",
			"get", "balance",
		},
		&stdout,
		&stderr,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		executeCLICommand,
	)

	expected := "Key: \"balance\"\n" +
		"Value: \"100\"\n" +
		"Found: true\n" +
		"Read mode: linearizable\n"

	if code != cliExitSuccess ||
		stdout.String() != expected ||
		stderr.Len() != 0 {
		t.Fatalf("unexpected output: %s", stdout.String())
	}
}

func TestCLIGetMissingTextOutput(t *testing.T) {
	kv := &cliGetTestClient{
		found: false,
	}

	var stdout, stderr bytes.Buffer

	code := runCLI(
		context.Background(),
		[]string{
			"--output", "text",
			"get", "--local", "missing",
		},
		&stdout,
		&stderr,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		executeCLICommand,
	)

	expected := "Key: \"missing\"\n" +
		"Found: false\n" +
		"Read mode: local\n"

	if code != cliExitNotFound ||
		stdout.String() != expected ||
		stderr.Len() != 0 {
		t.Fatalf("unexpected output: %s", stdout.String())
	}
}

func TestCLIGetClientErrors(t *testing.T) {
	cases := []error{
		errors.New("backend unavailable"),
		context.DeadlineExceeded,
		context.Canceled,
	}

	for _, failure := range cases {
		for _, local := range []bool{false, true} {
			kv := &cliGetTestClient{
				err: failure,
			}

			args := []string{"get", "key"}

			if local {
				args = []string{"get", "--local", "key"}
			}

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

			if code != cliExitFailure ||
				stdout.Len() != 0 ||
				!strings.Contains(
					stderr.String(),
					failure.Error(),
				) ||
				kv.closeCalls != 1 {
				t.Fatal("incorrect GET error handling")
			}
		}
	}
}

func TestCLIGetOutputFailure(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		for _, found := range []bool{true, false} {
			kv := &cliGetTestClient{
				value: "100",
				found: found,
			}

			var stderr bytes.Buffer

			code := runCLI(
				context.Background(),
				[]string{
					"--output", format,
					"get", "key",
				},
				cliGetFailingWriter{},
				&stderr,
				func(cliConfig) (cliKVClient, error) {
					return kv, nil
				},
				executeCLICommand,
			)

			if code != cliExitFailure ||
				!strings.Contains(
					stderr.String(),
					"output failed",
				) ||
				kv.closeCalls != 1 {
				t.Fatal("incorrect output failure handling")
			}
		}
	}
}

func TestCLIGetInvalidOutputFormat(t *testing.T) {
	err := writeCLIGetResult(
		io.Discard,
		"xml",
		cliGetResult{},
	)

	if err == nil {
		t.Fatal("expected unsupported output format error")
	}
}

func TestCLIGetContextPropagation(t *testing.T) {
	kv := &cliGetTestClient{
		value: "100",
		found: true,
	}

	code := runCLI(
		context.Background(),
		[]string{
			"--timeout", "5s",
			"get", "key",
		},
		io.Discard,
		io.Discard,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		executeCLICommand,
	)

	if code != cliExitSuccess ||
		kv.lastContext == nil {
		t.Fatal("GET did not receive operation context")
	}

	deadline, ok := kv.lastContext.Deadline()

	if !ok || time.Until(deadline) > 5*time.Second {
		t.Fatal("incorrect operation deadline")
	}

	if !errors.Is(
		kv.lastContext.Err(),
		context.Canceled,
	) {
		t.Fatal("operation context was not cleaned up")
	}
}

func TestCLIGetInvalidArgumentsNoClient(t *testing.T) {
	for _, args := range [][]string{
		{"get"},
		{"get", ""},
		{"get", "a", "b"},
		{"get", "--invalid", "key"},
	} {
		code := runCLI(
			context.Background(),
			args,
			io.Discard,
			io.Discard,
			func(cliConfig) (cliKVClient, error) {
				t.Fatal("invalid GET must not initialize client")
				return nil, nil
			},
			executeCLICommand,
		)

		if code != cliExitUsage {
			t.Fatalf("unexpected exit code: %d", code)
		}
	}
}
