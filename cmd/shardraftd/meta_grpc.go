package main

import (
	"context"
	"sort"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	metaTotalSlots   = uint32(65536)
	metaInitialEpoch = kvInitialEpoch
)

// metaGRPCServer exposes the static shard ownership map
// and the locally observed Raft leader.
type metaGRPCServer struct {
	shardraftpb.UnimplementedMetaServiceServer

	shardID       uint64
	epoch         uint64
	node          adminNode
	peerIDs       []raft.NodeID
	peerAddresses map[raft.NodeID]string
	localAddress  string
}

func newMetaGRPCServer(
	shardID uint64,
	node adminNode,
	peers []raft.NodeID,
	peerAddresses map[raft.NodeID]string,
	localAddress string,
) *metaGRPCServer {
	peerIDs := make([]raft.NodeID, 0, len(peers))
	addresses := make(map[raft.NodeID]string)
	seen := make(map[raft.NodeID]bool)

	for _, id := range peers {
		if id == 0 || seen[id] {
			continue
		}

		seen[id] = true
		peerIDs = append(peerIDs, id)

		if address := peerAddresses[id]; address != "" {
			addresses[id] = address
		}
	}

	sort.Slice(peerIDs, func(i, j int) bool {
		return peerIDs[i] < peerIDs[j]
	})

	return &metaGRPCServer{
		shardID:       shardID,
		epoch:         metaInitialEpoch,
		node:          node,
		peerIDs:       peerIDs,
		peerAddresses: addresses,
		localAddress:  localAddress,
	}
}

// shardInfo constructs an independent metadata snapshot.
// Leader information is advisory and does not prove quorum.
func (s *metaGRPCServer) shardInfo(
	ctx context.Context,
) (*shardraftpb.ShardInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
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

	if _, err := encodeAdminRole(nodeStatus.Role); err != nil {
		return nil, err
	}

	if nodeStatus.ID == 0 || applied > nodeStatus.Commit {
		return nil, status.Error(
			codes.Internal,
			"inconsistent node status",
		)
	}

	members := make(
		map[raft.NodeID]string,
		len(s.peerIDs)+1,
	)

	for _, id := range s.peerIDs {
		members[id] = s.peerAddresses[id]
	}

	members[nodeStatus.ID] = s.localAddress

	ids := make([]raft.NodeID, 0, len(members))

	for id := range members {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	replicas := make(
		[]*shardraftpb.ReplicaInfo,
		0,
		len(ids),
	)

	for _, id := range ids {
		replicas = append(
			replicas,
			&shardraftpb.ReplicaInfo{
				NodeId:  uint64(id),
				Address: members[id],
			},
		)
	}

	leaderID := nodeStatus.LeaderID

	switch nodeStatus.Role {
	case raft.Leader:
		leaderID = nodeStatus.ID
	case raft.Candidate:
		leaderID = 0
	}

	if _, exists := members[leaderID]; !exists {
		leaderID = 0
	}

	return &shardraftpb.ShardInfo{
		ShardId: s.shardID,
		Epoch:   s.epoch,
		SlotRanges: []*shardraftpb.SlotRange{
			{
				StartSlot: 0,
				EndSlot:   metaTotalSlots - 1,
			},
		},
		Replicas: replicas,
		LeaderId: uint64(leaderID),
	}, nil
}

// GetShardMap returns the complete static ownership map.
func (s *metaGRPCServer) GetShardMap(
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

	info, err := s.shardInfo(ctx)
	if err != nil {
		return nil, err
	}

	return &shardraftpb.GetShardMapResponse{
		ShardMap: &shardraftpb.ShardMap{
			Epoch:      s.epoch,
			TotalSlots: metaTotalSlots,
			Shards: []*shardraftpb.ShardInfo{
				info,
			},
		},
	}, nil
}

// GetShard returns metadata for a specific shard.
func (s *metaGRPCServer) GetShard(
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

	if request.ShardId != s.shardID {
		return &shardraftpb.GetShardResponse{
			Found: false,
		}, nil
	}

	info, err := s.shardInfo(ctx)
	if err != nil {
		return nil, err
	}

	return &shardraftpb.GetShardResponse{
		Shard: info,
		Found: true,
	}, nil
}

var _ shardraftpb.MetaServiceServer = (*metaGRPCServer)(nil)
