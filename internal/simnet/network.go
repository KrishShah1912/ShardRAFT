package simnet

import (
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// Deliver exchanges queued messages until the network is idle.
// Messages selected by HoldMessage are retained for later delivery.
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
			if c.DropMessage != nil && c.DropMessage(msg) {
				continue
			}

			if c.HoldMessage != nil && c.HoldMessage(msg) {
				c.HeldMessages = append(c.HeldMessages, msg)
				continue
			}

			if err := c.deliverMessage(msg); err != nil {
				return err
			}
		}
	}

	return fmt.Errorf("network did not become idle")
}

// deliverMessage sends one message to its destination.
func (c *Cluster) deliverMessage(msg raft.Message) error {
	target, ok := c.Nodes[msg.To]
	if !ok {
		return fmt.Errorf("unknown target node %d", msg.To)
	}

	return target.Step(msg)
}

// ReleaseHeld delivers all held messages in their original order.
// Newly generated responses can be processed with Deliver().
func (c *Cluster) ReleaseHeld() error {
	held := c.HeldMessages
	c.HeldMessages = nil

	for _, msg := range held {
		if err := c.deliverMessage(msg); err != nil {
			return err
		}
	}

	return nil
}

// ReleaseHeldReverse delivers held messages in reverse order.
// This allows deterministic testing of message reordering.
func (c *Cluster) ReleaseHeldReverse() error {
	held := c.HeldMessages
	c.HeldMessages = nil

	for i := len(held) - 1; i >= 0; i-- {
		if err := c.deliverMessage(held[i]); err != nil {
			return err
		}
	}

	return nil
}
