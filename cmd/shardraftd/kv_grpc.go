package main

import (
	"context"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

const (
	kvInitialEpoch   = uint64(1)
	kvMaxRequestSize = 1 << 20
)

type kvGRPCNode interface {
	StatusSnapshot() (raft.Status, uint64, error)

	Get(key string) (string, bool, error)

	GetLinearizable(
		ctx context.Context,
		key string,
	) (string, bool, error)

	Propose(command kvstore.Command) error

	CheckedRequestResult(
		command kvstore.Command,
	) (kvstore.RequestResult, bool, error)
}

type kvGRPCServer struct {
	shardraftpb.UnimplementedKVServiceServer

	shardID       uint64
	epoch         uint64
	node          kvGRPCNode
	peerAddresses map[raft.NodeID]string
}

func newKVGRPCServer(
	shardID uint64,
	node kvGRPCNode,
	peerAddresses map[raft.NodeID]string,
) *kvGRPCServer {
	addresses := make(
		map[raft.NodeID]string,
		len(peerAddresses),
	)

	for id, address := range peerAddresses {
		if id != 0 && address != "" {
			addresses[id] = address
		}
	}

	return &kvGRPCServer{
		shardID:       shardID,
		epoch:         kvInitialEpoch,
		node:          node,
		peerAddresses: addresses,
	}
}

func (s *kvGRPCServer) routingError(
	code shardraftpb.ErrorCode,
	message string,
	nodeStatus raft.Status,
) *shardraftpb.RoutingError {
	result := &shardraftpb.RoutingError{
		Code:    code,
		Message: message,
		Term:    nodeStatus.Term,
		ShardId: s.shardID,
		Epoch:   s.epoch,
	}

	if nodeStatus.LeaderID != 0 {
		result.LeaderId = uint64(nodeStatus.LeaderID)
		result.LeaderAddress =
			s.peerAddresses[nodeStatus.LeaderID]
	}

	return result
}

var _ kvGRPCNode = (*shard.Shard)(nil)

var _ shardraftpb.KVServiceServer = (*kvGRPCServer)(nil)
