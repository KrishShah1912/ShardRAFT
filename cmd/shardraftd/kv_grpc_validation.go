package main

import (
	"unicode/utf8"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/protobuf/proto"
)

func (s *kvGRPCServer) invalidRequest(
	message string,
) *shardraftpb.RoutingError {
	return s.routingError(
		shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		message,
		raft.Status{},
	)
}

func (s *kvGRPCServer) validateHeader(
	header *shardraftpb.RequestHeader,
	mutation bool,
) *shardraftpb.RoutingError {
	if header == nil {
		return s.invalidRequest(
			"request header is required",
		)
	}

	if header.ShardId == 0 {
		return s.invalidRequest(
			"shard ID is required",
		)
	}

	if header.ShardId != s.shardID {
		return s.routingError(
			shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			"unsupported shard ID",
			raft.Status{},
		)
	}

	if header.Epoch != 0 &&
		header.Epoch != s.epoch {
		return s.routingError(
			shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			"unsupported routing epoch",
			raft.Status{},
		)
	}

	if !utf8.ValidString(header.ClientId) ||
		!utf8.ValidString(header.RequestId) {
		return s.invalidRequest(
			"request identifiers must be valid UTF-8",
		)
	}

	hasClient := header.ClientId != ""
	hasRequest := header.RequestId != ""

	if hasClient != hasRequest {
		return s.invalidRequest(
			"client ID and request ID must be paired",
		)
	}

	if mutation && !hasClient {
		return s.invalidRequest(
			"mutations require client ID and request ID",
		)
	}

	return nil
}

func (s *kvGRPCServer) validateKey(
	key string,
) *shardraftpb.RoutingError {
	if key == "" {
		return s.invalidRequest(
			"key cannot be empty",
		)
	}

	if !utf8.ValidString(key) {
		return s.invalidRequest(
			"key must be valid UTF-8",
		)
	}

	return nil
}

func (s *kvGRPCServer) validateRequestSize(
	request proto.Message,
) *shardraftpb.RoutingError {
	if request == nil ||
		!request.ProtoReflect().IsValid() {
		return s.invalidRequest(
			"request is required",
		)
	}

	if proto.Size(request) > kvMaxRequestSize {
		return s.invalidRequest(
			"request exceeds maximum size",
		)
	}

	return nil
}

func (s *kvGRPCServer) validateReadMode(
	mode shardraftpb.ReadMode,
) *shardraftpb.RoutingError {
	switch mode {
	case shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		shardraftpb.ReadMode_READ_MODE_LOCAL:
		return nil

	default:
		return s.invalidRequest(
			"unsupported read mode",
		)
	}
}

func (s *kvGRPCServer) validateGetRequest(
	request *shardraftpb.GetRequest,
) *shardraftpb.RoutingError {
	if request == nil {
		return s.invalidRequest(
			"get request is required",
		)
	}

	if failure := s.validateRequestSize(request); failure != nil {
		return failure
	}

	if failure := s.validateHeader(
		request.Header,
		false,
	); failure != nil {
		return failure
	}

	if failure := s.validateKey(request.Key); failure != nil {
		return failure
	}

	return s.validateReadMode(request.ReadMode)
}

func (s *kvGRPCServer) validateMutationCommand(
	command kvstore.Command,
) *shardraftpb.RoutingError {
	if failure := s.validateKey(command.Key); failure != nil {
		return failure
	}

	if command.ClientID == "" ||
		command.RequestID == "" {
		return s.invalidRequest(
			"mutations require client ID and request ID",
		)
	}

	if !utf8.ValidString(command.Value) ||
		!utf8.ValidString(command.Expected) ||
		!utf8.ValidString(command.ClientID) ||
		!utf8.ValidString(command.RequestID) {
		return s.invalidRequest(
			"mutation fields must be valid UTF-8",
		)
	}

	switch command.Op {
	case kvstore.OpPut,
		kvstore.OpDelete,
		kvstore.OpCompareAndSwap:
	default:
		return s.invalidRequest(
			"unsupported mutation operation",
		)
	}

	if err := command.Validate(); err != nil {
		return s.invalidRequest(
			"invalid mutation command",
		)
	}

	return nil
}
