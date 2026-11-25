package main

import (
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

// kvGRPCPeerAddresses builds the address map used by
// the KV service to return gRPC leader routing hints.
func kvGRPCPeerAddresses(
	peers []transport.Peer,
) map[raft.NodeID]string {
	addresses := make(map[raft.NodeID]string, len(peers))

	for _, peer := range peers {
		if peer.ID == 0 || peer.Address == "" {
			continue
		}

		addresses[peer.ID] = peer.Address
	}

	return addresses
}
