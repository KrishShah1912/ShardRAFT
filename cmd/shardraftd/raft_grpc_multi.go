package main

import (
	"context"
	"errors"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type multiRaftGRPCServer struct {
	shardraftpb.UnimplementedRaftServiceServer

	metadata *meta.Group
	manager  *shard.Manager
	nodeID   raft.NodeID
}

func newMultiRaftGRPCServer(
	manager *shard.Manager,
	nodeID raft.NodeID,
) *multiRaftGRPCServer {
	return &multiRaftGRPCServer{
		manager: manager,
		nodeID:  nodeID,
	}
}

func (s *multiRaftGRPCServer) Step(
	ctx context.Context,
	request *shardraftpb.StepRequest,
) (*shardraftpb.StepResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if request == nil || request.Message == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"Raft message is required",
		)
	}

	if request.ShardId == 0 {
		return nil, status.Error(
			codes.FailedPrecondition,
			"shard ID is required",
		)
	}

	if request.ShardId == uint64(meta.MetadataGroupID) {
		if s.metadata == nil {
			return nil, status.Error(
				codes.FailedPrecondition,
				"metadata group is not hosted locally",
			)
		}

		var peers []raft.NodeID

		for _, id := range meta.MetadataReplicas() {
			if id != s.nodeID {
				peers = append(peers, id)
			}
		}

		return newRaftGRPCServer(
			request.ShardId,
			s.metadata,
			peers,
		).Step(ctx, request)
	}

	if s.manager == nil {
		return nil, status.Error(
			codes.Unavailable,
			"group manager unavailable",
		)
	}

	groupID := meta.ShardID(request.ShardId)

	if uint64(groupID) != request.ShardId {
		return nil, status.Error(
			codes.FailedPrecondition,
			"invalid shard ID",
		)
	}

	group, err := s.manager.Group(groupID)

	if errors.Is(err, shard.ErrGroupNotLocal) {
		return nil, status.Error(
			codes.FailedPrecondition,
			"shard is not hosted locally",
		)
	}

	if err != nil {
		return nil, status.Error(
			codes.Unavailable,
			"shard unavailable",
		)
	}

	for _, placement := range s.manager.ShardMap().Shards {
		if placement.ID != groupID {
			continue
		}

		var peers []raft.NodeID
		local := false

		for _, replica := range placement.Replicas {
			if replica == s.nodeID {
				local = true
				continue
			}

			peers = append(peers, replica)
		}

		if !local {
			return nil, status.Error(
				codes.FailedPrecondition,
				"node is not a shard replica",
			)
		}

		handler := newRaftGRPCServer(
			request.ShardId,
			group.Shard(),
			peers,
		)

		return handler.Step(ctx, request)
	}

	return nil, status.Error(
		codes.FailedPrecondition,
		"unknown shard",
	)
}

var _ shardraftpb.RaftServiceServer = (*multiRaftGRPCServer)(nil)
