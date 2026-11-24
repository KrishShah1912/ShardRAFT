package simnet

import (
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// SimReadState associates a confirmed read barrier
// with the simulated node that produced it.
//
// A collected confirmation is not an application-level
// read result.
type SimReadState struct {
	NodeID raft.NodeID
	State  raft.ReadState
}

// Bound the simulator's unconsumed confirmation queue.
const maxCollectedReadStates = 4096

// collectReadStates transfers confirmed read barriers
// into simulator-owned storage.
//
// Only successfully collected confirmations are
// acknowledged to the originating Raft node.
func (c *Cluster) collectReadStates(
	id raft.NodeID,
	states []raft.ReadState,
) error {
	if len(states) == 0 {
		return nil
	}

	node, exists := c.Nodes[id]

	if !exists || node == nil {
		return fmt.Errorf(
			"unknown read-state source node %d", id,
		)
	}

	status := node.Status()

	c.pruneObsoleteReadStates()

	if len(states) >
		maxCollectedReadStates-len(c.readStates) {
		return fmt.Errorf(
			"simulator read-state queue capacity exceeded",
		)
	}

	// Validate the complete batch before collecting it.
	for _, state := range states {
		if state.RequestID == 0 ||
			state.Term == 0 ||
			state.Index == 0 ||
			len(state.Context) == 0 {
			return fmt.Errorf(
				"invalid read state from node %d", id,
			)
		}

		if status.Role != raft.Leader ||
			state.Term != status.Term ||
			state.Index > status.Commit {
			return fmt.Errorf(
				"obsolete read state from node %d", id,
			)
		}
	}

	for _, state := range states {
		copied := state

		copied.Context = append(
			[]byte(nil),
			state.Context...,
		)

		c.readStates = append(
			c.readStates,
			SimReadState{
				NodeID: id,
				State:  copied,
			},
		)
	}

	node.AdvanceReadStates(states)

	return nil
}

// ReadStates returns independent copies of collected
// confirmations that match local leadership and have
// satisfied the simulated application barrier.
//
// It does not consume the simulator queue.
func (c *Cluster) ReadStates() []SimReadState {
	var result []SimReadState

	for _, item := range c.readStates {
		if !c.readStateCurrent(item) ||
			!c.readBarrierSatisfied(item) {
			continue
		}

		copied := item

		copied.State.Context = append(
			[]byte(nil),
			item.State.Context...,
		)

		result = append(result, copied)
	}

	return result
}

// DrainReadStates transfers application-ready confirmations.
//
// Unsatisfied barriers are retained. Obsolete confirmations
// are discarded.
func (c *Cluster) DrainReadStates() []SimReadState {
	states := c.ReadStates()

	remaining := c.readStates[:0]

	for _, item := range c.readStates {
		if c.readStateCurrent(item) &&
			!c.readBarrierSatisfied(item) {
			remaining = append(remaining, item)
		}
	}

	clear(c.readStates[len(remaining):])
	c.readStates = remaining

	if len(c.readStates) == 0 {
		c.readStates = nil
	}

	return states
}
