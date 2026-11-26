package client

import (
	"context"
	"errors"
	"testing"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryCommittedConflictResolution(t *testing.T) {
	tests := []struct {
		name        string
		code        shardraftpb.ErrorCode
		confirmed   bool
		wantUnknown bool
	}{
		{
			name:      "CommittedConflict",
			code:      shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
			confirmed: true,
		},
		{
			name:        "PendingConflict",
			code:        shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
			wantUnknown: true,
		},
		{
			name:        "LegacyServerConflict",
			code:        shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
			wantUnknown: true,
		},
		{
			name:        "InvalidRequest",
			code:        shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
			wantUnknown: true,
		},
		{
			name:        "InvalidConfirmationFlag",
			code:        shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
			confirmed:   true,
			wantUnknown: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executor := newTestRetryExecutor(t)
			identity := testRetryIdentity()
			attempts := 0

			err := executor.Execute(
				context.Background(),
				true,
				identity,
				func(
					ctx context.Context,
					attempt int,
					gotIdentity MutationIdentity,
				) (*shardraftpb.RoutingError, error) {
					attempts++

					if gotIdentity != identity {
						t.Fatal("Mutation identity changed")
					}

					if attempt == 1 {
						return nil, status.Error(
							codes.Unavailable,
							"old leader unavailable",
						)
					}

					return &shardraftpb.RoutingError{
						Code:              tc.code,
						ShardId:           1,
						Epoch:             1,
						ConflictCommitted: tc.confirmed,
					}, nil
				},
			)

			if err == nil {
				t.Fatal("Expected routing failure")
			}

			if attempts != 2 {
				t.Fatalf("attempts=%d want=2", attempts)
			}

			if got := errors.Is(
				err,
				ErrMutationOutcomeUnknown,
			); got != tc.wantUnknown {
				t.Fatalf(
					"outcome_unknown=%v want=%v err=%v",
					got, tc.wantUnknown, err,
				)
			}

			var failure *RoutingFailure

			if !errors.As(err, &failure) {
				t.Fatalf("Missing routing failure: %v", err)
			}

			if failure.Detail.GetCode() != tc.code {
				t.Fatalf("Unexpected routing code: %v", failure)
			}

			t.Logf(
				"attempts=%d confirmed=%v unknown=%v",
				attempts,
				tc.confirmed,
				tc.wantUnknown,
			)
		})
	}
}

func TestRetryConfirmedConflictWithTransportError(t *testing.T) {
	executor := newTestRetryExecutor(t)
	identity := testRetryIdentity()
	attempts := 0

	err := executor.Execute(
		context.Background(),
		true,
		identity,
		func(
			ctx context.Context,
			attempt int,
			gotIdentity MutationIdentity,
		) (*shardraftpb.RoutingError, error) {
			attempts++

			if gotIdentity != identity {
				t.Fatal("Mutation identity changed")
			}

			return &shardraftpb.RoutingError{
				Code:              shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
				ConflictCommitted: true,
			}, status.Error(
				codes.Unavailable,
				"transport failure",
			)
		},
	)

	if !errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("Transport uncertainty lost: %v", err)
	}

	if !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("Expected retry exhaustion: %v", err)
	}

	if attempts != 5 {
		t.Fatalf("attempts=%d want=5", attempts)
	}
}

func TestRetryConflictWithoutPreviousAmbiguity(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		executor := newTestRetryExecutor(t)
		attempts := 0

		err := executor.Execute(
			context.Background(),
			true,
			testRetryIdentity(),
			func(
				ctx context.Context,
				attempt int,
				identity MutationIdentity,
			) (*shardraftpb.RoutingError, error) {
				attempts++

				return &shardraftpb.RoutingError{
					Code:              shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
					ConflictCommitted: confirmed,
				}, nil
			},
		)

		var failure *RoutingFailure

		if !errors.As(err, &failure) {
			t.Fatalf("Expected routing failure: %v", err)
		}

		if errors.Is(err, ErrMutationOutcomeUnknown) {
			t.Fatal("Unexpected mutation uncertainty")
		}

		if attempts != 1 {
			t.Fatalf("attempts=%d want=1", attempts)
		}
	}
}
