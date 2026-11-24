package simnet

import (
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// ReadIndex submits a read confirmation request to a
// simulated Raft node.
//
// Successful admission does not imply quorum confirmation
// or state-machine application.
//
// The caller must explicitly advance simulated network
// delivery using Deliver and Tick.
func (c *Cluster) ReadIndex(
	id raft.NodeID,
	ctx []byte,
) error {
	node, exists := c.Nodes[id]

	if !exists || node == nil {
		return fmt.Errorf("unknown read target node %d", id)
	}

	return node.ReadIndex(ctx)
}
