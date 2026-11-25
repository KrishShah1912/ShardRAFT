package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestRetryExecutor(t testing.TB) *RetryExecutor {
	t.Helper()

	executor, err := NewRetryExecutor(RetryConfig{
		MaxAttempts:    5,
		AttemptTimeout: time.Second,
		InitialBackoff: 0,
		MaxBackoff:     0,
	})

	if err != nil {
		t.Fatal(err)
	}

	return executor
}

func testRetryIdentity() MutationIdentity {
	return MutationIdentity{
		ClientID:  "retry-test-client",
		RequestID: "request-001",
	}
}

func testRetryRoutingError(
	code shardraftpb.ErrorCode,
) *shardraftpb.RoutingError {
	return &shardraftpb.RoutingError{
		Code:    code,
		ShardId: 1,
		Epoch:   1,
	}
}

func TestRetryDefaultConfiguration(t *testing.T) {
	if err := DefaultRetryConfig().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRetryInvalidConfiguration(t *testing.T) {
	configs := []RetryConfig{
		{},
		{MaxAttempts: -1},
		{MaxAttempts: 1, AttemptTimeout: 0},
		{
			MaxAttempts:    1,
			AttemptTimeout: time.Second,
			InitialBackoff: -time.Second,
		},
		{
			MaxAttempts:    1,
			AttemptTimeout: time.Second,
			InitialBackoff: time.Second,
			MaxBackoff:     time.Millisecond,
		},
	}

	for _, config := range configs {
		_, err := NewRetryExecutor(config)

		if !errors.Is(err, ErrInvalidRetryConfig) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestRetryMutationIdentity(t *testing.T) {
	identity, err := NewMutationIdentity("client-001")

	if err != nil {
		t.Fatal(err)
	}

	if identity.ClientID != "client-001" ||
		len(identity.RequestID) != 32 {
		t.Fatal("invalid generated identity")
	}

	first := identity.Header(1, 1)
	second := identity.Header(1, 2)

	if first.GetClientId() != second.GetClientId() ||
		first.GetRequestId() != second.GetRequestId() {
		t.Fatal("mutation identity changed")
	}

	if first.GetEpoch() != 1 || second.GetEpoch() != 2 {
		t.Fatal("routing epoch was not updated")
	}
}

func TestRetryInvalidIdentity(t *testing.T) {
	for _, identity := range []MutationIdentity{
		{},
		{ClientID: "client"},
		{RequestID: "request"},
	} {
		if !errors.Is(
			identity.Validate(),
			ErrInvalidMutationIdentity,
		) {
			t.Fatal("invalid identity accepted")
		}
	}

	_, err := NewMutationIdentity("")

	if !errors.Is(err, ErrInvalidMutationIdentity) {
		t.Fatal(err)
	}
}

func TestRetryConcurrentRequestIDs(t *testing.T) {
	const workers = 32
	const requests = 1000

	var wg sync.WaitGroup
	var seen sync.Map

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < requests; j++ {
				id, err := NewRequestID()

				if err != nil {
					t.Error(err)
					return
				}

				if _, exists := seen.LoadOrStore(id, true); exists {
					t.Error("duplicate request ID")
					return
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("validated %d generated request IDs", workers*requests)
}

func TestRetryRoutingClassification(t *testing.T) {
	tests := []struct {
		code      shardraftpb.ErrorCode
		retry     bool
		ambiguous bool
	}{
		{shardraftpb.ErrorCode_ERROR_CODE_OK, false, false},
		{shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER, true, false},
		{shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD, true, false},
		{shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE, true, false},
		{shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN, true, true},
		{shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT, false, false},
		{shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false, false},
		{shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND, false, false},
		{shardraftpb.ErrorCode_ERROR_CODE_INTERNAL, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			decision := ClassifyRetry(
				testRetryRoutingError(tt.code),
				nil,
			)

			if decision.Retry != tt.retry ||
				decision.Ambiguous != tt.ambiguous {
				t.Fatalf("unexpected decision: %+v", decision)
			}
		})
	}

	if decision := ClassifyRetry(nil, nil); decision.Retry ||
		decision.Ambiguous {
		t.Fatal("successful response classified incorrectly")
	}
}

func TestRetryTransportClassification(t *testing.T) {
	tests := []struct {
		code      codes.Code
		retry     bool
		ambiguous bool
	}{
		{codes.Unavailable, true, true},
		{codes.DeadlineExceeded, true, true},
		{codes.ResourceExhausted, true, true},
		{codes.Canceled, false, true},
		{codes.Unknown, false, true},
		{codes.Internal, false, true},
		{codes.DataLoss, false, true},
		{codes.InvalidArgument, false, false},
		{codes.PermissionDenied, false, false},
		{codes.Unauthenticated, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			decision := ClassifyRetry(
				nil,
				status.Error(tt.code, "test"),
			)

			if decision.Retry != tt.retry ||
				decision.Ambiguous != tt.ambiguous {
				t.Fatalf("unexpected decision: %+v", decision)
			}
		})
	}
}

func TestRetryImmediateSuccess(t *testing.T) {
	executor := newTestRetryExecutor(t)
	calls := 0

	err := executor.Execute(
		context.Background(),
		false,
		MutationIdentity{},
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			calls++
			return nil, nil
		},
	)

	if err != nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRetryExplicitOK(t *testing.T) {
	executor := newTestRetryExecutor(t)

	err := executor.Execute(
		context.Background(),
		false,
		MutationIdentity{},
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			return testRetryRoutingError(
				shardraftpb.ErrorCode_ERROR_CODE_OK,
			), nil
		},
	)

	if err != nil {
		t.Fatal(err)
	}
}

func TestRetryStableMutationIdentity(t *testing.T) {
	executor := newTestRetryExecutor(t)
	identity := testRetryIdentity()
	calls := 0

	err := executor.Execute(
		context.Background(),
		true,
		identity,
		func(ctx context.Context, attempt int, received MutationIdentity) (*shardraftpb.RoutingError, error) {
			calls++

			if received != identity || attempt != calls {
				t.Fatal("identity or attempt number changed incorrectly")
			}

			switch attempt {
			case 1:
				return testRetryRoutingError(
					shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
				), nil

			case 2:
				return testRetryRoutingError(
					shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
				), nil

			case 3:
				return nil, status.Error(
					codes.Unavailable,
					"connection lost",
				)

			default:
				return nil, nil
			}
		},
	)

	if err != nil || calls != 4 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRetryBoundedAttempts(t *testing.T) {
	executor := newTestRetryExecutor(t)
	calls := 0

	err := executor.Execute(
		context.Background(),
		false,
		MutationIdentity{},
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			calls++

			return testRetryRoutingError(
				shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			), nil
		},
	)

	if calls != 5 || !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRetryMutationOutcomeUnknown(t *testing.T) {
	executor := newTestRetryExecutor(t)

	err := executor.Execute(
		context.Background(),
		true,
		testRetryIdentity(),
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			return testRetryRoutingError(
				shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
			), nil
		},
	)

	if !errors.Is(err, ErrRetryExhausted) ||
		!errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryAmbiguousTransportFailure(t *testing.T) {
	executor := newTestRetryExecutor(t)

	err := executor.Execute(
		context.Background(),
		true,
		testRetryIdentity(),
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			return nil, status.Error(
				codes.Unavailable,
				"connection lost",
			)
		},
	)

	if !errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryPreservesEarlierAmbiguity(t *testing.T) {
	executor := newTestRetryExecutor(t)

	err := executor.Execute(
		context.Background(),
		true,
		testRetryIdentity(),
		func(ctx context.Context, attempt int, identity MutationIdentity) (*shardraftpb.RoutingError, error) {
			if attempt == 1 {
				return nil, status.Error(
					codes.Unavailable,
					"connection lost",
				)
			}

			return testRetryRoutingError(
				shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			), nil
		},
	)

	if !errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatal("earlier ambiguous outcome was lost")
	}
}

func TestRetryDefiniteRejection(t *testing.T) {
	executor := newTestRetryExecutor(t)

	err := executor.Execute(
		context.Background(),
		true,
		testRetryIdentity(),
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			return testRetryRoutingError(
				shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			), nil
		},
	)

	if !errors.Is(err, ErrRetryExhausted) ||
		errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryPermanentFailure(t *testing.T) {
	executor := newTestRetryExecutor(t)
	calls := 0

	err := executor.Execute(
		context.Background(),
		true,
		testRetryIdentity(),
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			calls++

			return testRetryRoutingError(
				shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
			), nil
		},
	)

	var failure *RoutingFailure

	if calls != 1 ||
		!errors.As(err, &failure) ||
		errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRetryCanceledBeforeAttempt(t *testing.T) {
	executor := newTestRetryExecutor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := executor.Execute(
		ctx,
		true,
		testRetryIdentity(),
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			t.Fatal("unexpected attempt")
			return nil, nil
		},
	)

	if !errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryCanceledAfterDispatch(t *testing.T) {
	executor := newTestRetryExecutor(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := executor.Execute(
		ctx,
		true,
		testRetryIdentity(),
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			cancel()
			return nil, status.Error(codes.Canceled, "canceled")
		},
	)

	if !errors.Is(err, context.Canceled) ||
		!errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryAttemptDeadline(t *testing.T) {
	executor, err := NewRetryExecutor(RetryConfig{
		MaxAttempts:    2,
		AttemptTimeout: 10 * time.Millisecond,
	})

	if err != nil {
		t.Fatal(err)
	}

	calls := 0

	err = executor.Execute(
		context.Background(),
		true,
		testRetryIdentity(),
		func(ctx context.Context, attempt int, identity MutationIdentity) (*shardraftpb.RoutingError, error) {
			calls++
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		},
	)

	if calls != 2 ||
		!errors.Is(err, ErrRetryExhausted) ||
		!errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRetryBackoff(t *testing.T) {
	maximum := 100 * time.Millisecond

	delay := 25 * time.Millisecond

	for _, expected := range []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
	} {
		delay = retryBackoff(delay, maximum)

		if delay != expected {
			t.Fatalf("delay=%v expected=%v", delay, expected)
		}
	}

	if retryBackoff(75*time.Millisecond, maximum) != maximum {
		t.Fatal("backoff exceeded maximum")
	}
}

func TestRetryBackoffCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := waitRetry(ctx, time.Second); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatal(err)
	}
}

func TestRetryOverallDeadline(t *testing.T) {
	executor, err := NewRetryExecutor(RetryConfig{
		MaxAttempts:    5,
		AttemptTimeout: time.Second,
		InitialBackoff: time.Second,
		MaxBackoff:     time.Second,
	})

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()

	calls := 0

	err = executor.Execute(
		ctx,
		false,
		MutationIdentity{},
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			calls++

			return testRetryRoutingError(
				shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			), nil
		},
	)

	if calls > 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRetryRejectsInvalidMutation(t *testing.T) {
	executor := newTestRetryExecutor(t)

	err := executor.Execute(
		context.Background(),
		true,
		MutationIdentity{},
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			t.Fatal("unexpected mutation attempt")
			return nil, nil
		},
	)

	if !errors.Is(err, ErrInvalidMutationIdentity) {
		t.Fatal(err)
	}
}

func TestRetryRejectsNilCallback(t *testing.T) {
	executor := newTestRetryExecutor(t)

	err := executor.Execute(
		context.Background(),
		false,
		MutationIdentity{},
		nil,
	)

	if !errors.Is(err, ErrInvalidRetryConfig) {
		t.Fatal(err)
	}
}

func TestRetryReadDoesNotReportMutationAmbiguity(t *testing.T) {
	executor := newTestRetryExecutor(t)

	err := executor.Execute(
		context.Background(),
		false,
		MutationIdentity{},
		func(context.Context, int, MutationIdentity) (*shardraftpb.RoutingError, error) {
			return nil, status.Error(codes.Unavailable, "unavailable")
		},
	)

	if !errors.Is(err, ErrRetryExhausted) ||
		errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func BenchmarkRetryImmediateSuccess(b *testing.B) {
	executor := newTestRetryExecutor(b)
	ctx := context.Background()

	callback := func(
		context.Context,
		int,
		MutationIdentity,
	) (*shardraftpb.RoutingError, error) {
		return nil, nil
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := executor.Execute(
			ctx,
			false,
			MutationIdentity{},
			callback,
		); err != nil {
			b.Fatal(err)
		}
	}
}
