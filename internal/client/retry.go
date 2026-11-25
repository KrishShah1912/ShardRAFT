package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidRetryConfig      = errors.New("invalid retry configuration")
	ErrRetryExhausted          = errors.New("retry attempts exhausted")
	ErrMutationOutcomeUnknown  = errors.New("mutation outcome unknown")
	ErrInvalidMutationIdentity = errors.New("invalid mutation identity")
)

type RetryConfig struct {
	MaxAttempts    int
	AttemptTimeout time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:    5,
		AttemptTimeout: 2 * time.Second,
		InitialBackoff: 25 * time.Millisecond,
		MaxBackoff:     250 * time.Millisecond,
	}
}

func (cfg RetryConfig) Validate() error {
	if cfg.MaxAttempts <= 0 ||
		cfg.AttemptTimeout <= 0 ||
		cfg.InitialBackoff < 0 ||
		cfg.MaxBackoff < cfg.InitialBackoff {
		return ErrInvalidRetryConfig
	}

	return nil
}

// MutationIdentity remains unchanged across retry attempts.
type MutationIdentity struct {
	ClientID  string
	RequestID string
}

func (identity MutationIdentity) Validate() error {
	if identity.ClientID == "" || identity.RequestID == "" {
		return ErrInvalidMutationIdentity
	}

	return nil
}

func NewRequestID() (string, error) {
	var value [16]byte

	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(value[:]), nil
}

func NewMutationIdentity(
	clientID string,
) (MutationIdentity, error) {
	if clientID == "" {
		return MutationIdentity{}, ErrInvalidMutationIdentity
	}

	requestID, err := NewRequestID()

	if err != nil {
		return MutationIdentity{}, err
	}

	return MutationIdentity{
		ClientID:  clientID,
		RequestID: requestID,
	}, nil
}

// Header preserves the mutation identity while allowing
// routing metadata to change between attempts.
func (identity MutationIdentity) Header(
	shardID uint64,
	epoch uint64,
) *shardraftpb.RequestHeader {
	return &shardraftpb.RequestHeader{
		ClientId:  identity.ClientID,
		RequestId: identity.RequestID,
		ShardId:   shardID,
		Epoch:     epoch,
	}
}

type RetryDecision struct {
	Retry     bool
	Ambiguous bool
}

func ClassifyRetry(
	routingError *shardraftpb.RoutingError,
	rpcError error,
) RetryDecision {
	if rpcError != nil {
		switch status.Code(rpcError) {
		case codes.Unavailable,
			codes.DeadlineExceeded,
			codes.ResourceExhausted:
			return RetryDecision{
				Retry:     true,
				Ambiguous: true,
			}

		case codes.Canceled,
			codes.Unknown,
			codes.Internal,
			codes.DataLoss:
			return RetryDecision{
				Ambiguous: true,
			}

		default:
			return RetryDecision{}
		}
	}

	if routingError == nil {
		return RetryDecision{}
	}

	switch routingError.GetCode() {
	case shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
		shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
		shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE:
		return RetryDecision{
			Retry: true,
		}

	case shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN:
		return RetryDecision{
			Retry:     true,
			Ambiguous: true,
		}

	case shardraftpb.ErrorCode_ERROR_CODE_INTERNAL:
		return RetryDecision{
			Ambiguous: true,
		}

	default:
		return RetryDecision{}
	}
}

type RoutingFailure struct {
	Detail *shardraftpb.RoutingError
}

func (failure *RoutingFailure) Error() string {
	return fmt.Sprintf(
		"routing failure: %s: %s",
		failure.Detail.GetCode().String(),
		failure.Detail.GetMessage(),
	)
}

type RetryAttempt func(
	ctx context.Context,
	attempt int,
	identity MutationIdentity,
) (*shardraftpb.RoutingError, error)

type RetryExecutor struct {
	config RetryConfig
}

func NewRetryExecutor(
	config RetryConfig,
) (*RetryExecutor, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &RetryExecutor{
		config: config,
	}, nil
}

func retryBackoff(
	current time.Duration,
	maximum time.Duration,
) time.Duration {
	if current >= maximum {
		return maximum
	}

	if current > maximum-current {
		return maximum
	}

	return current * 2
}

func waitRetry(
	ctx context.Context,
	delay time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-timer.C:
		return ctx.Err()
	}
}

func retryResult(
	mutation bool,
	ambiguous bool,
	err error,
) error {
	if mutation && ambiguous {
		return errors.Join(
			ErrMutationOutcomeUnknown,
			err,
		)
	}

	return err
}

// Execute runs a bounded number of attempts.
//
// A mutation identity is supplied once and reused for every
// attempt. The callback may refresh routing metadata but
// must not change the mutation payload.
func (executor *RetryExecutor) Execute(
	ctx context.Context,
	mutation bool,
	identity MutationIdentity,
	attempt RetryAttempt,
) error {
	if attempt == nil {
		return ErrInvalidRetryConfig
	}

	if mutation {
		if err := identity.Validate(); err != nil {
			return err
		}
	}

	var lastError error
	ambiguous := false
	backoff := executor.config.InitialBackoff

	for number := 1; number <= executor.config.MaxAttempts; number++ {
		if err := ctx.Err(); err != nil {
			return retryResult(
				mutation,
				ambiguous,
				errors.Join(lastError, err),
			)
		}

		attemptCtx, cancel := context.WithTimeout(
			ctx,
			executor.config.AttemptTimeout,
		)

		routingError, rpcError := attempt(
			attemptCtx,
			number,
			identity,
		)

		cancel()

		if rpcError == nil &&
			(routingError == nil ||
				routingError.GetCode() ==
					shardraftpb.ErrorCode_ERROR_CODE_OK) {
			return nil
		}

		decision := ClassifyRetry(
			routingError,
			rpcError,
		)

		ambiguous = ambiguous || decision.Ambiguous

		if rpcError != nil {
			lastError = rpcError
		} else {
			lastError = &RoutingFailure{
				Detail: routingError,
			}
		}

		if err := ctx.Err(); err != nil {
			return retryResult(
				mutation,
				ambiguous,
				errors.Join(lastError, err),
			)
		}

		if !decision.Retry {
			return retryResult(
				mutation,
				ambiguous,
				lastError,
			)
		}

		if number == executor.config.MaxAttempts {
			break
		}

		if err := waitRetry(ctx, backoff); err != nil {
			return retryResult(
				mutation,
				ambiguous,
				errors.Join(lastError, err),
			)
		}

		backoff = retryBackoff(
			backoff,
			executor.config.MaxBackoff,
		)
	}

	return retryResult(
		mutation,
		ambiguous,
		errors.Join(
			ErrRetryExhausted,
			lastError,
		),
	)
}
