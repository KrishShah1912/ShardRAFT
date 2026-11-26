package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/client"
)

type cliLifecycleTestClient struct {
	cliKVClient
	closeCalls int
	closeErr   error
}

func (c *cliLifecycleTestClient) Close() error {
	c.closeCalls++
	return c.closeErr
}

func TestCLIRunHelp(t *testing.T) {
	for _, args := range [][]string{
		{"--help"},
		{"help"},
		{"get", "--help"},
	} {
		var stdout, stderr bytes.Buffer

		code := runCLI(
			context.Background(),
			args,
			&stdout,
			&stderr,
			nil,
			nil,
		)

		if code != cliExitSuccess {
			t.Fatalf("unexpected exit code: %d", code)
		}

		if !strings.Contains(stdout.String(), "Usage:") {
			t.Fatal("missing help output")
		}

		if stderr.Len() != 0 {
			t.Fatalf("unexpected stderr: %s", stderr.String())
		}
	}
}

func TestCLIRunInvalidArguments(t *testing.T) {
	cases := [][]string{
		nil,
		{"unknown"},
		{"get"},
		{"--invalid"},
		{"put", "key"},
	}

	for _, args := range cases {
		var stdout, stderr bytes.Buffer

		code := runCLI(
			context.Background(),
			args,
			&stdout,
			&stderr,
			nil,
			nil,
		)

		if code != cliExitUsage {
			t.Fatalf("unexpected exit code: %d", code)
		}

		if stdout.Len() != 0 || stderr.Len() == 0 {
			t.Fatal("invalid output stream behavior")
		}
	}
}

func TestCLIRunSuccessfulLifecycle(t *testing.T) {
	kv := &cliLifecycleTestClient{}
	factoryCalls := 0
	executeCalls := 0

	var stdout, stderr bytes.Buffer

	factory := func(cfg cliConfig) (cliKVClient, error) {
		factoryCalls++

		if cfg.Timeout != 5*time.Second {
			t.Fatalf("unexpected timeout: %v", cfg.Timeout)
		}

		if cfg.Output != "text" {
			t.Fatalf("unexpected output: %s", cfg.Output)
		}

		return kv, nil
	}

	execute := func(
		ctx context.Context,
		got cliKVClient,
		invocation cliInvocation,
		out io.Writer,
		_ io.Writer,
	) int {
		executeCalls++

		if got != kv {
			t.Fatal("incorrect client instance")
		}

		if invocation.Command.Operation != cliGet ||
			invocation.Command.Key != "balance" {
			t.Fatalf("unexpected command: %+v", invocation.Command)
		}

		deadline, ok := ctx.Deadline()

		if !ok || time.Until(deadline) <= 0 ||
			time.Until(deadline) > 5*time.Second {
			t.Fatal("incorrect operation deadline")
		}

		_, _ = io.WriteString(out, "success\n")

		return cliExitSuccess
	}

	code := runCLI(
		context.Background(),
		[]string{
			"--timeout", "5s",
			"--output", "text",
			"get", "balance",
		},
		&stdout,
		&stderr,
		factory,
		execute,
	)

	if code != cliExitSuccess {
		t.Fatalf("unexpected exit code: %d", code)
	}

	if factoryCalls != 1 ||
		executeCalls != 1 ||
		kv.closeCalls != 1 {
		t.Fatalf(
			"factory=%d execute=%d close=%d",
			factoryCalls,
			executeCalls,
			kv.closeCalls,
		)
	}

	if stdout.String() != "success\n" || stderr.Len() != 0 {
		t.Fatal("unexpected output")
	}
}

func TestCLIRunFactoryFailure(t *testing.T) {
	var stderr bytes.Buffer

	code := runCLI(
		context.Background(),
		[]string{"get", "key"},
		io.Discard,
		&stderr,
		func(cliConfig) (cliKVClient, error) {
			return nil, errors.New("initialization failed")
		},
		func(
			context.Context,
			cliKVClient,
			cliInvocation,
			io.Writer,
			io.Writer,
		) int {
			t.Fatal("executor must not run")
			return cliExitFailure
		},
	)

	if code != cliExitFailure ||
		!strings.Contains(stderr.String(), "initialization failed") {
		t.Fatal("expected initialization failure")
	}
}

func TestCLIRunNilClient(t *testing.T) {
	var stderr bytes.Buffer

	code := runCLI(
		context.Background(),
		[]string{"get", "key"},
		io.Discard,
		&stderr,
		func(cliConfig) (cliKVClient, error) {
			return nil, nil
		},
		executeCLICommand,
	)

	if code != cliExitFailure ||
		!strings.Contains(stderr.String(), "returned no client") {
		t.Fatal("expected nil client rejection")
	}
}

func TestCLIRunMissingDependencies(t *testing.T) {
	var stderr bytes.Buffer

	code := runCLI(
		context.Background(),
		[]string{"get", "key"},
		io.Discard,
		&stderr,
		nil,
		nil,
	)

	if code != cliExitFailure ||
		!strings.Contains(stderr.String(), "invalid CLI execution") {
		t.Fatal("expected dependency validation failure")
	}
}

func TestCLIRunCleanupOnFailure(t *testing.T) {
	kv := &cliLifecycleTestClient{}

	code := runCLI(
		context.Background(),
		[]string{"get", "key"},
		io.Discard,
		io.Discard,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		func(
			context.Context,
			cliKVClient,
			cliInvocation,
			io.Writer,
			io.Writer,
		) int {
			return cliExitFailure
		},
	)

	if code != cliExitFailure || kv.closeCalls != 1 {
		t.Fatal("client must close after execution failure")
	}
}

func TestCLIRunCleanupError(t *testing.T) {
	for _, result := range []int{
		cliExitSuccess,
		cliExitFailure,
		cliExitUsage,
	} {
		kv := &cliLifecycleTestClient{
			closeErr: errors.New("close failed"),
		}

		var stderr bytes.Buffer

		code := runCLI(
			context.Background(),
			[]string{"get", "key"},
			io.Discard,
			&stderr,
			func(cliConfig) (cliKVClient, error) {
				return kv, nil
			},
			func(
				context.Context,
				cliKVClient,
				cliInvocation,
				io.Writer,
				io.Writer,
			) int {
				return result
			},
		)

		expected := result

		if expected == cliExitSuccess {
			expected = cliExitFailure
		}

		if code != expected ||
			kv.closeCalls != 1 ||
			!strings.Contains(stderr.String(), "close failed") {
			t.Fatal("incorrect cleanup error handling")
		}
	}
}

func TestCLIRunCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var stderr bytes.Buffer

	code := runCLI(
		ctx,
		[]string{"get", "key"},
		io.Discard,
		&stderr,
		func(cliConfig) (cliKVClient, error) {
			t.Fatal("factory must not run")
			return nil, nil
		},
		executeCLICommand,
	)

	if code != cliExitFailure ||
		!strings.Contains(stderr.String(), "context canceled") {
		t.Fatal("expected context cancellation")
	}
}

func TestCLIRunCancellationDuringInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kv := &cliLifecycleTestClient{}

	code := runCLI(
		ctx,
		[]string{"get", "key"},
		io.Discard,
		io.Discard,
		func(cliConfig) (cliKVClient, error) {
			cancel()
			return kv, nil
		},
		func(
			context.Context,
			cliKVClient,
			cliInvocation,
			io.Writer,
			io.Writer,
		) int {
			t.Fatal("executor must not run")
			return cliExitFailure
		},
	)

	if code != cliExitFailure || kv.closeCalls != 1 {
		t.Fatal("expected cancellation and client cleanup")
	}
}

func TestCLIRunParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	parentDeadline, _ := ctx.Deadline()
	kv := &cliLifecycleTestClient{}

	code := runCLI(
		ctx,
		[]string{"--timeout", "30s", "get", "key"},
		io.Discard,
		io.Discard,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		func(
			operationCtx context.Context,
			_ cliKVClient,
			_ cliInvocation,
			_ io.Writer,
			_ io.Writer,
		) int {
			deadline, ok := operationCtx.Deadline()

			if !ok || deadline.After(parentDeadline) {
				t.Fatal("parent deadline must be preserved")
			}

			return cliExitSuccess
		},
	)

	if code != cliExitSuccess || kv.closeCalls != 1 {
		t.Fatal("unexpected lifecycle result")
	}
}

func TestCLIRunContextCleanup(t *testing.T) {
	kv := &cliLifecycleTestClient{}
	var operationCtx context.Context

	code := runCLI(
		context.Background(),
		[]string{"get", "key"},
		io.Discard,
		io.Discard,
		func(cliConfig) (cliKVClient, error) {
			return kv, nil
		},
		func(
			ctx context.Context,
			_ cliKVClient,
			_ cliInvocation,
			_ io.Writer,
			_ io.Writer,
		) int {
			operationCtx = ctx
			return cliExitSuccess
		},
	)

	if code != cliExitSuccess ||
		operationCtx == nil ||
		!errors.Is(operationCtx.Err(), context.Canceled) {
		t.Fatal("operation context must be canceled after execution")
	}
}

func TestCLINewClient(t *testing.T) {
	cfg := defaultCLIConfig()
	cfg.ClientID = "explicit-client"

	kv, err := newCLIClient(cfg)

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	}()

	identity, err := kv.NewMutationIdentity()

	if err != nil {
		t.Fatal(err)
	}

	if identity.ClientID != "explicit-client" ||
		identity.RequestID == "" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func TestCLINewClientInvalidConfig(t *testing.T) {
	cfg := defaultCLIConfig()
	cfg.Endpoints = nil

	kv, err := newCLIClient(cfg)

	if err == nil || kv != nil {
		t.Fatal("expected invalid configuration rejection")
	}
}

func TestCLINewClientAutomaticIdentity(t *testing.T) {
	kv, err := newCLIClient(defaultCLIConfig())

	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	}()

	first, err := kv.NewMutationIdentity()

	if err != nil {
		t.Fatal(err)
	}

	second, err := kv.NewMutationIdentity()

	if err != nil {
		t.Fatal(err)
	}

	if first.ClientID == "" ||
		first.ClientID != second.ClientID ||
		first.RequestID == "" ||
		second.RequestID == "" ||
		first.RequestID == second.RequestID {
		t.Fatal("incorrect generated identities")
	}

	if err := first.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCLIClientInterfaceCompatibility(t *testing.T) {
	var _ cliKVClient = (*client.Client)(nil)
}

func TestCLIUnsupportedDispatcher(t *testing.T) {
	for _, operation := range []cliOperation{
		cliOperation("unsupported"),
		cliOperation(""),
	} {
		var stdout, stderr bytes.Buffer

		code := executeCLICommand(
			context.Background(),
			nil,
			cliInvocation{
				Command: cliCommand{
					Operation: operation,
				},
			},
			&stdout,
			&stderr,
		)

		if code != cliExitFailure ||
			stdout.Len() != 0 ||
			!strings.Contains(stderr.String(), "unsupported command") {
			t.Fatal("unexpected initial dispatcher behavior")
		}
	}
}
