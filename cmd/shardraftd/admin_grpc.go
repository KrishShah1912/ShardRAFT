package main

import (
	"context"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// adminNode exposes the local state required by AdminService.
type adminNode interface {
	StatusSnapshot() (raft.Status, uint64, error)
}

type adminGRPCServer struct {
	shardraftpb.UnimplementedAdminServiceServer

	shardID uint64
	node    adminNode
}

func newAdminGRPCServer(
	shardID uint64,
	node adminNode,
) *adminGRPCServer {
	return &adminGRPCServer{
		shardID: shardID,
		node:    node,
	}
}

// encodeAdminRole explicitly converts internal Raft roles
// into their protobuf equivalents.
func encodeAdminRole(
	role raft.Role,
) (shardraftpb.NodeRole, error) {
	switch role {
	case raft.Follower:
		return shardraftpb.NodeRole_NODE_ROLE_FOLLOWER, nil

	case raft.Candidate:
		return shardraftpb.NodeRole_NODE_ROLE_CANDIDATE, nil

	case raft.Leader:
		return shardraftpb.NodeRole_NODE_ROLE_LEADER, nil

	default:
		return 0, status.Error(
			codes.Internal,
			"unknown Raft role",
		)
	}
}

// Status returns a synchronized snapshot of local node state.
func (s *adminGRPCServer) Status(
	ctx context.Context,
	request *shardraftpb.StatusRequest,
) (*shardraftpb.StatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"status request is required",
		)
	}

	if request.ShardId == 0 ||
		request.ShardId != s.shardID {
		return nil, status.Error(
			codes.FailedPrecondition,
			"wrong shard",
		)
	}

	if s.node == nil {
		return nil, status.Error(
			codes.Unavailable,
			"node unavailable",
		)
	}

	nodeStatus, applied, err := s.node.StatusSnapshot()

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, status.FromContextError(ctxErr).Err()
	}

	if err != nil {
		return nil, status.Error(
			codes.Unavailable,
			"node unavailable",
		)
	}

	role, err := encodeAdminRole(nodeStatus.Role)
	if err != nil {
		return nil, err
	}

	if nodeStatus.ID == 0 ||
		applied > nodeStatus.Commit {
		return nil, status.Error(
			codes.Internal,
			"inconsistent node status",
		)
	}

	return &shardraftpb.StatusResponse{
		Id:           uint64(nodeStatus.ID),
		Role:         role,
		Term:         nodeStatus.Term,
		LeaderId:     uint64(nodeStatus.LeaderID),
		CommitIndex:  nodeStatus.Commit,
		AppliedIndex: applied,
		ShardId:      s.shardID,
	}, nil
}

// Health reports local shard availability.
// It does not establish leadership or quorum availability.
func (s *adminGRPCServer) Health(
	ctx context.Context,
	request *shardraftpb.HealthRequest,
) (*shardraftpb.HealthResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"health request is required",
		)
	}

	if s.node == nil {
		return &shardraftpb.HealthResponse{
			Healthy: false,
			Message: "node unavailable",
		}, nil
	}

	nodeStatus, applied, err := s.node.StatusSnapshot()

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, status.FromContextError(ctxErr).Err()
	}

	if err != nil {
		return &shardraftpb.HealthResponse{
			Healthy: false,
			Message: "node unavailable",
		}, nil
	}

	_, roleErr := encodeAdminRole(nodeStatus.Role)

	if roleErr != nil ||
		nodeStatus.ID == 0 ||
		applied > nodeStatus.Commit {
		return &shardraftpb.HealthResponse{
			Healthy: false,
			Message: "inconsistent node status",
		}, nil
	}

	return &shardraftpb.HealthResponse{
		Healthy: true,
		Message: "node operational",
	}, nil
}

var _ shardraftpb.AdminServiceServer = (*adminGRPCServer)(nil)
