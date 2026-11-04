package simnet

import (
	"sort"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type Cluster struct {
	Nodes map[raft.NodeID]*raft.Raft
	IDs   []raft.NodeID
}

// NewCluster creates nodes with different election timeouts.
func NewCluster(ids []raft.NodeID) *Cluster {
	c := &Cluster{
		Nodes: make(map[raft.NodeID]*raft.Raft),
		IDs:   append([]raft.NodeID(nil), ids...),
	}

	sort.Slice(c.IDs, func(i, j int) bool {
		return c.IDs[i] < c.IDs[j]
	})

	for i, id := range c.IDs {
		var peers []raft.NodeID

		for _, other := range c.IDs {
			if other != id {
				peers = append(peers, other)
			}
		}

		timeout := 5 + i*3
		c.Nodes[id] = raft.NewRaft(id, peers, timeout)
	}

	return c
}

// Tick advances simulated time for every node.
func (c *Cluster) Tick() {
	for _, id := range c.IDs {
		c.Nodes[id].Tick()
	}
}
