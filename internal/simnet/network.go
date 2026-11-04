package simnet

import (
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// Deliver exchanges queued messages until the network is idle.
func (c *Cluster) Deliver() error {
	for round := 0; round < 1000; round++ {
		var messages []raft.Message

		for _, id := range c.IDs {
			node := c.Nodes[id]
			ready := node.Ready()

			messages = append(messages, ready.Messages...)
			node.Advance()
		}

		if len(messages) == 0 {
			return nil
		}

		for _, msg := range messages {
			target, ok := c.Nodes[msg.To]
			if !ok {
				return fmt.Errorf("unknown target node %d", msg.To)
			}

			if err := target.Step(msg); err != nil {
				return err
			}
		}
	}

	return fmt.Errorf("network did not become idle")
}
