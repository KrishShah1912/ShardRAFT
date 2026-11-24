package main

import (
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/status"
)

const (
	kvMutationTimeout      = 3 * time.Second
	kvMutationPollInterval = 10 * time.Millisecond
)

func (s *kvGRPCServer) mutationFailure(
	code shardraftpb.ErrorCode,
	message string,
) *shardraftpb.MutationResponse {
	return &shardraftpb.MutationResponse{
		Error: s.routingError(
			code,
			message,
			s.currentRoutingStatus(),
		),
	}
}

func (s *kvGRPCServer) unknownMutation() *shardraftpb.MutationResponse {
	return s.mutationFailure(
		shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
		"commit not confirmed; request outcome unknown",
	)
}

func (s *kvGRPCServer) mutationError(
	err error,
	submitted bool,
) *shardraftpb.MutationResponse {
	switch {
	case errors.Is(err, kvstore.ErrRequestConflict):
		return s.mutationFailure(
			shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
			"request identity conflicts with existing command",
		)

	case submitted:
		return s.unknownMutation()

	case errors.Is(err, raft.ErrNotLeader):
		return s.mutationFailure(
			shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			"not leader",
		)

	case errors.Is(err, shard.ErrClosed),
		errors.Is(err, shard.ErrStopped):
		return s.mutationFailure(
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
			"node unavailable",
		)

	default:
		return s.mutationFailure(
			shardraftpb.ErrorCode_ERROR_CODE_INTERNAL,
			"mutation failed",
		)
	}
}

func (s *kvGRPCServer) completedMutation(
	result kvstore.RequestResult,
) *shardraftpb.MutationResponse {
	if !utf8.ValidString(result.Value) {
		return s.mutationFailure(
			shardraftpb.ErrorCode_ERROR_CODE_INTERNAL,
			"stored result is not valid UTF-8",
		)
	}

	return &shardraftpb.MutationResponse{
		Applied: result.Applied,
		Value:   result.Value,
	}
}

func (s *kvGRPCServer) executeMutation(
	ctx context.Context,
	command kvstore.Command,
) (*shardraftpb.MutationResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if failure := s.validateMutationCommand(command); failure != nil {
		return &shardraftpb.MutationResponse{
			Error: failure,
		}, nil
	}

	if s.node == nil {
		return s.mutationFailure(
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
			"node unavailable",
		), nil
	}

	nodeStatus, _, err := s.node.StatusSnapshot()

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, status.FromContextError(ctxErr).Err()
	}

	if err != nil {
		return s.mutationFailure(
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
			"node unavailable",
		), nil
	}

	if nodeStatus.Role != raft.Leader {
		return &shardraftpb.MutationResponse{
			Error: s.routingError(
				shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
				"not leader",
				nodeStatus,
			),
		}, nil
	}

	// Return the original result of an identical applied request.
	result, found, err := s.node.CheckedRequestResult(command)

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, status.FromContextError(ctxErr).Err()
	}

	if err != nil {
		return s.mutationError(err, false), nil
	}

	if found {
		return s.completedMutation(result), nil
	}

	// Check cancellation immediately before submitting.
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	err = s.node.Propose(command)

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, status.FromContextError(ctxErr).Err()
	}

	if err != nil &&
		!errors.Is(err, shard.ErrRequestPending) {

		if errors.Is(err, raft.ErrNotLeader) ||
			errors.Is(err, kvstore.ErrRequestConflict) {
			return s.mutationError(err, false), nil
		}

		// Other proposal errors may occur after the command
		// has entered the Raft log.
		return s.unknownMutation(), nil
	}

	// A proposal or pending request is not a commit acknowledgement.
	timeout := time.NewTimer(kvMutationTimeout)
	defer timeout.Stop()

	poll := time.NewTicker(kvMutationPollInterval)
	defer poll.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		result, found, err := s.node.CheckedRequestResult(command)

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, status.FromContextError(ctxErr).Err()
		}

		if err != nil {
			return s.mutationError(err, true), nil
		}

		if found {
			return s.completedMutation(result), nil
		}

		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()

		case <-timeout.C:
			return s.unknownMutation(), nil

		case <-poll.C:
		}
	}
}
