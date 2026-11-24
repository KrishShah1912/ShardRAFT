package main

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/status"
)

func (s *kvGRPCServer) currentRoutingStatus() raft.Status {
	if s.node == nil {
		return raft.Status{}
	}

	nodeStatus, _, err := s.node.StatusSnapshot()
	if err != nil {
		return raft.Status{}
	}

	return nodeStatus
}

func (s *kvGRPCServer) readFailure(
	err error,
) *shardraftpb.RoutingError {
	var code shardraftpb.ErrorCode
	var message string

	switch {
	case errors.Is(err, raft.ErrNotLeader):
		code = shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER
		message = "not leader"

	case errors.Is(err, raft.ErrReadNotReady),
		errors.Is(err, raft.ErrTooManyPendingReads),
		errors.Is(err, shard.ErrTooManyPendingReads),
		errors.Is(err, shard.ErrClosed),
		errors.Is(err, shard.ErrStopped),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):

		code = shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE
		message = "read unavailable"

	case errors.Is(err, shard.ErrInvalidReadKey):
		code = shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST
		message = "invalid read key"

	default:
		code = shardraftpb.ErrorCode_ERROR_CODE_INTERNAL
		message = "read failed"
	}

	return s.routingError(
		code,
		message,
		s.currentRoutingStatus(),
	)
}

func (s *kvGRPCServer) Get(
	ctx context.Context,
	request *shardraftpb.GetRequest,
) (*shardraftpb.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if failure := s.validateGetRequest(request); failure != nil {
		return &shardraftpb.GetResponse{
			Error: failure,
		}, nil
	}

	if s.node == nil {
		return &shardraftpb.GetResponse{
			Key: request.Key,
			Error: s.routingError(
				shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
				"node unavailable",
				raft.Status{},
			),
		}, nil
	}

	var value string
	var found bool
	var err error

	switch request.ReadMode {
	case shardraftpb.ReadMode_READ_MODE_LINEARIZABLE:
		value, found, err = s.node.GetLinearizable(
			ctx,
			request.Key,
		)

	case shardraftpb.ReadMode_READ_MODE_LOCAL:
		value, found, err = s.node.Get(request.Key)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, status.FromContextError(ctxErr).Err()
	}

	if err != nil {
		failure := s.readFailure(err)

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, status.FromContextError(ctxErr).Err()
		}

		return &shardraftpb.GetResponse{
			Key:   request.Key,
			Error: failure,
		}, nil
	}

	if !found {
		failure := s.routingError(
			shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND,
			"key not found",
			s.currentRoutingStatus(),
		)

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, status.FromContextError(ctxErr).Err()
		}

		return &shardraftpb.GetResponse{
			Key:   request.Key,
			Found: false,
			Error: failure,
		}, nil
	}

	if !utf8.ValidString(value) {
		return &shardraftpb.GetResponse{
			Key: request.Key,
			Error: s.routingError(
				shardraftpb.ErrorCode_ERROR_CODE_INTERNAL,
				"stored value is not valid UTF-8",
				raft.Status{},
			),
		}, nil
	}

	return &shardraftpb.GetResponse{
		Key:   request.Key,
		Value: value,
		Found: true,
	}, nil
}
