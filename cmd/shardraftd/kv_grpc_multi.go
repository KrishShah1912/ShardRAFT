package main

import (
	"context"
	"errors"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type multiKVGRPCServer struct {
	shardraftpb.UnimplementedKVServiceServer

	manager   *shard.Manager
	addresses map[raft.NodeID]string
}

func newMultiKVGRPCServer(
	manager *shard.Manager,
	addresses map[raft.NodeID]string,
) *multiKVGRPCServer {
	copied := make(map[raft.NodeID]string)

	for id, address := range addresses {
		copied[id] = address
	}

	return &multiKVGRPCServer{
		manager:   manager,
		addresses: copied,
	}
}

func (s *multiKVGRPCServer) resolve(
	request proto.Message,
	header *shardraftpb.RequestHeader,
	key string,
	mutation bool,
) (*kvGRPCServer, *shardraftpb.RoutingError) {
	validator := newKVGRPCServer(
		header.GetShardId(),
		nil,
		s.addresses,
	)

	if failure := validator.validateRequestSize(request); failure != nil {
		return nil, failure
	}

	if header == nil || header.GetShardId() == 0 {
		return nil, validator.invalidRequest(
			"nonzero shard ID is required",
		)
	}

	if failure := validator.validateKey(key); failure != nil {
		return nil, failure
	}

	if s.manager == nil {
		return nil, validator.routingError(
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
			"shard manager unavailable",
			raft.Status{},
		)
	}

	layout := s.manager.ShardMap()

	owner, err := layout.OwnerForSlot(
		meta.SlotForKey(key),
	)

	if err != nil {
		return nil, validator.routingError(
			shardraftpb.ErrorCode_ERROR_CODE_INTERNAL,
			"cannot resolve shard ownership",
			raft.Status{},
		)
	}

	target := newKVGRPCServer(
		uint64(owner.ID),
		nil,
		s.addresses,
	)

	target.epoch = layout.Version

	if failure := target.validateHeader(
		header,
		mutation,
	); failure != nil {
		return nil, failure
	}

	group, err := s.manager.GroupForKey(key)

	if err != nil {
		if errors.Is(err, shard.ErrClosed) {
			return nil, target.routingError(
				shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
				"shard manager unavailable",
				raft.Status{},
			)
		}

		var notLocal *shard.NotLocalError

		if errors.As(err, &notLocal) {
			return nil, target.routingError(
				shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
				"owning shard is not hosted locally",
				raft.Status{},
			)
		}

		return nil, target.routingError(
			shardraftpb.ErrorCode_ERROR_CODE_INTERNAL,
			"shard lookup failed",
			raft.Status{},
		)
	}

	if group.ID() != owner.ID {
		return nil, target.routingError(
			shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			"shard ownership mismatch",
			raft.Status{},
		)
	}

	target.node = group.Shard()

	return target, nil
}

func (s *multiKVGRPCServer) Get(
	ctx context.Context,
	request *shardraftpb.GetRequest,
) (*shardraftpb.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	target, failure := s.resolve(
		request,
		request.GetHeader(),
		request.GetKey(),
		false,
	)

	if failure != nil {
		return &shardraftpb.GetResponse{
			Key:   request.GetKey(),
			Error: failure,
		}, nil
	}

	return target.Get(ctx, request)
}

func (s *multiKVGRPCServer) Put(
	ctx context.Context,
	request *shardraftpb.PutRequest,
) (*shardraftpb.MutationResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	target, failure := s.resolve(
		request,
		request.GetHeader(),
		request.GetKey(),
		true,
	)

	if failure != nil {
		return &shardraftpb.MutationResponse{
			Error: failure,
		}, nil
	}

	return target.Put(ctx, request)
}

func (s *multiKVGRPCServer) Delete(
	ctx context.Context,
	request *shardraftpb.DeleteRequest,
) (*shardraftpb.MutationResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	target, failure := s.resolve(
		request,
		request.GetHeader(),
		request.GetKey(),
		true,
	)

	if failure != nil {
		return &shardraftpb.MutationResponse{
			Error: failure,
		}, nil
	}

	return target.Delete(ctx, request)
}

func (s *multiKVGRPCServer) CompareAndSwap(
	ctx context.Context,
	request *shardraftpb.CompareAndSwapRequest,
) (*shardraftpb.MutationResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	target, failure := s.resolve(
		request,
		request.GetHeader(),
		request.GetKey(),
		true,
	)

	if failure != nil {
		return &shardraftpb.MutationResponse{
			Error: failure,
		}, nil
	}

	return target.CompareAndSwap(ctx, request)
}

var _ shardraftpb.KVServiceServer = (*multiKVGRPCServer)(nil)
