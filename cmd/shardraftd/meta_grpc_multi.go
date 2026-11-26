package main

import (
	"context"
	"sort"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type multiMetaGRPCServer struct {
	shardraftpb.UnimplementedMetaServiceServer

	manager   *shard.Manager
	metadata  *meta.Group
	addresses map[raft.NodeID]string
}

func newMultiMetaGRPCServer(
	manager *shard.Manager,
	metadata *meta.Group,
	addresses map[raft.NodeID]string,
) *multiMetaGRPCServer {
	copied := make(map[raft.NodeID]string)

	for id, address := range addresses {
		copied[id] = address
	}

	return &multiMetaGRPCServer{
		manager:   manager,
		metadata:  metadata,
		addresses: copied,
	}
}

func (s *multiMetaGRPCServer) shardMap(
	ctx context.Context,
) (*shardraftpb.ShardMap, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if s.metadata == nil || s.manager == nil {
		return nil, status.Error(
			codes.Unavailable,
			"local metadata group unavailable",
		)
	}

	layout, err := s.metadata.Current()

	if err != nil || layout.Version == 0 {
		return nil, status.Error(
			codes.Unavailable,
			"committed metadata unavailable",
		)
	}

	if err := layout.Validate(); err != nil {
		return nil, status.Error(
			codes.Internal,
			"invalid committed shard map",
		)
	}

	result := &shardraftpb.ShardMap{
		Epoch:      layout.Version,
		TotalSlots: metaTotalSlots,
	}

	for _, placement := range layout.Shards {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		info := &shardraftpb.ShardInfo{
			ShardId: uint64(placement.ID),
			Epoch:   layout.Version,
			SlotRanges: []*shardraftpb.SlotRange{
				{
					StartSlot: placement.Range.Start,
					EndSlot:   placement.Range.End,
				},
			},
		}

		members := make(map[raft.NodeID]bool)

		ids := append(
			[]raft.NodeID(nil),
			placement.Replicas...,
		)

		sort.Slice(ids, func(i, j int) bool {
			return ids[i] < ids[j]
		})

		for _, id := range ids {
			address := s.addresses[id]

			if address == "" {
				return nil, status.Error(
					codes.Unavailable,
					"replica endpoint unavailable",
				)
			}

			members[id] = true

			info.Replicas = append(
				info.Replicas,
				&shardraftpb.ReplicaInfo{
					NodeId:  uint64(id),
					Address: address,
				},
			)
		}

		// Leader information is advisory.
		// Non-local groups have no local leader observation.
		group, err := s.manager.Group(placement.ID)

		if err == nil {
			state, err := group.Shard().Status()

			if err == nil {
				leader := state.LeaderID

				switch state.Role {
				case raft.Leader:
					leader = state.ID
				case raft.Candidate:
					leader = 0
				}

				if members[leader] {
					info.LeaderId = uint64(leader)
				}
			}
		}

		result.Shards = append(result.Shards, info)
	}

	sort.Slice(result.Shards, func(i, j int) bool {
		return result.Shards[i].ShardId <
			result.Shards[j].ShardId
	})

	return result, nil
}

func (s *multiMetaGRPCServer) GetShardMap(
	ctx context.Context,
	request *shardraftpb.GetShardMapRequest,
) (*shardraftpb.GetShardMapResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"shard map request is required",
		)
	}

	layout, err := s.shardMap(ctx)

	if err != nil {
		return nil, err
	}

	return &shardraftpb.GetShardMapResponse{
		ShardMap: layout,
	}, nil
}

func (s *multiMetaGRPCServer) GetShard(
	ctx context.Context,
	request *shardraftpb.GetShardRequest,
) (*shardraftpb.GetShardResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if request == nil || request.ShardId == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"nonzero shard ID is required",
		)
	}

	layout, err := s.shardMap(ctx)

	if err != nil {
		return nil, err
	}

	for _, info := range layout.Shards {
		if info.ShardId == request.ShardId {
			return &shardraftpb.GetShardResponse{
				Shard: info,
				Found: true,
			}, nil
		}
	}

	return &shardraftpb.GetShardResponse{
		Found: false,
	}, nil
}

var _ shardraftpb.MetaServiceServer = (*multiMetaGRPCServer)(nil)
