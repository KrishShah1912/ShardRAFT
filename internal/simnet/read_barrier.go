package simnet

import (
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// simApplication tracks simulated state-machine progress
// independently of Raft's internal applied index.
type simApplication struct {
	applied uint64
	paused  bool
	pending []raft.Entry
}

func (c *Cluster) applicationFor(
	id raft.NodeID,
) *simApplication {
	if c.applications == nil {
		c.applications = make(
			map[raft.NodeID]*simApplication,
		)
	}

	app := c.applications[id]

	if app == nil {
		app = &simApplication{}
		c.applications[id] = app
	}

	return app
}

// AppliedIndex returns simulated state-machine progress.
func (c *Cluster) AppliedIndex(
	id raft.NodeID,
) (uint64, error) {
	if c.Nodes[id] == nil {
		return 0, fmt.Errorf(
			"unknown application node %d", id,
		)
	}

	return c.applicationFor(id).applied, nil
}

// PauseApplication delays simulated state-machine
// application without preventing Raft communication.
func (c *Cluster) PauseApplication(
	id raft.NodeID,
) error {
	if c.Nodes[id] == nil {
		return fmt.Errorf(
			"unknown application node %d", id,
		)
	}

	c.applicationFor(id).paused = true

	return nil
}

// ResumeApplication applies queued committed entries
// in their original order.
func (c *Cluster) ResumeApplication(
	id raft.NodeID,
) error {
	if c.Nodes[id] == nil {
		return fmt.Errorf(
			"unknown application node %d", id,
		)
	}

	app := c.applicationFor(id)
	app.paused = false

	return app.applyPending()
}

// acceptCommittedEntries retains committed work before
// Raft.Advance acknowledges the corresponding Ready.
func (c *Cluster) acceptCommittedEntries(
	id raft.NodeID,
	entries []raft.Entry,
) error {
	app := c.applicationFor(id)

	last := app.applied

	if n := len(app.pending); n > 0 {
		last = app.pending[n-1].Index
	}

	// Validate the entire batch before modifying state.
	for _, entry := range entries {
		if entry.Index != last+1 {
			return fmt.Errorf(
				"node %d: noncontiguous application index %d after %d",
				id, entry.Index, last,
			)
		}

		last = entry.Index
	}

	for _, entry := range entries {
		copied := entry

		copied.Data = append(
			[]byte(nil),
			entry.Data...,
		)

		app.pending = append(
			app.pending,
			copied,
		)
	}

	if app.paused {
		return nil
	}

	return app.applyPending()
}

func (app *simApplication) applyPending() error {
	last := app.applied

	for _, entry := range app.pending {
		if entry.Index != last+1 {
			return fmt.Errorf(
				"noncontiguous pending application",
			)
		}

		last = entry.Index
	}

	app.applied = last

	clear(app.pending)
	app.pending = nil

	return nil
}

func (c *Cluster) readBarrierSatisfied(
	item SimReadState,
) bool {
	app := c.applications[item.NodeID]

	return app != nil &&
		item.State.Index != 0 &&
		app.applied >= item.State.Index
}

// readStateCurrent verifies that a confirmation still
// belongs to the node's current local leadership.
func (c *Cluster) readStateCurrent(
	item SimReadState,
) bool {
	node := c.Nodes[item.NodeID]

	if node == nil {
		return false
	}

	status := node.Status()

	return status.Role == raft.Leader &&
		status.Term == item.State.Term &&
		status.Commit >= item.State.Index
}
