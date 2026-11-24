package main

import (
	"context"
	"errors"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	"github.com/KrishShah1912/shardraft/internal/transport"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type raftGRPCServer struct {
	shardraftpb.UnimplementedRaftServiceServer

	shardID uint64
	node    raftMessageHandler
	peers   map[raft.NodeID]struct{}
}

func newRaftGRPCServer(
	shardID uint64,
	node raftMessageHandler,
	peers []raft.NodeID,
) *raftGRPCServer {
	allowed := make(map[raft.NodeID]struct{}, len(peers))

	for _, peer := range peers {
		allowed[peer] = struct{}{}
	}

	return &raftGRPCServer{
		shardID: shardID,
		node:    node,
		peers:   allowed,
	}
}

func (s *raftGRPCServer) Step(
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

	if request.ShardId == 0 ||
		request.ShardId != s.shardID {
		return nil, status.Error(
			codes.FailedPrecondition,
			"wrong shard",
		)
	}

	message, err := transport.DecodeRaftMessage(
		request.Message,
	)
	if err != nil {
		return nil, status.Error(
			codes.InvalidArgument,
			err.Error(),
		)
	}

	if message.From == 0 ||
		message.To == 0 ||
		message.From == message.To {
		return nil, status.Error(
			codes.InvalidArgument,
			"invalid Raft message endpoints",
		)
	}

	if _, allowed := s.peers[message.From]; !allowed {
		return nil, status.Error(
			codes.PermissionDenied,
			"sender is not a configured peer",
		)
	}

	if s.node == nil {
		return nil, status.Error(
			codes.Unavailable,
			"node unavailable",
		)
	}

	nodeStatus, err := s.node.Status()
	if err != nil {
		return nil, status.Error(
			codes.Unavailable,
			"node unavailable",
		)
	}

	if message.To != nodeStatus.ID ||
		message.From == nodeStatus.ID {
		return nil, status.Error(
			codes.InvalidArgument,
			"invalid Raft message destination",
		)
	}

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if err := s.node.Step(message); err != nil {
		if errors.Is(err, shard.ErrClosed) ||
			errors.Is(err, shard.ErrStopped) {
			return nil, status.Error(
				codes.Unavailable,
				"node unavailable",
			)
		}

		return nil, status.Error(
			codes.Internal,
			"Raft processing failed",
		)
	}

	return &shardraftpb.StepResponse{
		Accepted: true,
	}, nil
}

var _ shardraftpb.RaftServiceServer = (*raftGRPCServer)(nil)
