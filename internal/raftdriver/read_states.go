package raftdriver

import (
	"errors"
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// ReadStateFunc consumes a quorum-confirmed read barrier.
//
// The callback must not mutate the Raft node or reenter
// the driver. A successful return acknowledges delivery,
// not completion of a client-facing linearizable read.
type ReadStateFunc func(state raft.ReadState) error

type readStateAdvancer interface {
	AdvanceReadStates(states []raft.ReadState)
}

var ErrReadStateUnsupported = errors.New(
	"consensus node does not support read-state acknowledgment",
)

// SetReadStateHandler configures optional read-state
// delivery without changing the existing constructor.
//
// The caller must serialize configuration and processing.
// A nil handler disables delivery and preserves queued
// read confirmations.
func (d *Driver) SetReadStateHandler(
	handler ReadStateFunc,
) error {
	if d.stopped {
		return ErrStopped
	}

	if handler != nil {
		if _, ok := d.node.(readStateAdvancer); !ok {
			return ErrReadStateUnsupported
		}
	}

	d.readStateHandler = handler

	return nil
}

// processReadStates delivers confirmations after
// successful persistence, application, advancement, and sending.
func (d *Driver) processReadStates(
	states []raft.ReadState,
) error {
	if len(states) == 0 ||
		d.readStateHandler == nil {
		return nil
	}

	advancer, ok := d.node.(readStateAdvancer)

	if !ok {
		return ErrReadStateUnsupported
	}

	// Reject malformed identities and duplicate confirmations
	// before invoking any consumer.
	type readIdentity struct {
		id   uint64
		term uint64
	}

	seen := make(map[readIdentity]struct{}, len(states))

	for _, state := range states {
		if state.RequestID == 0 ||
			state.Term == 0 ||
			len(state.Context) == 0 {
			return fmt.Errorf(
				"invalid read-state identity: request=%d term=%d",
				state.RequestID,
				state.Term,
			)
		}

		key := readIdentity{
			id:   state.RequestID,
			term: state.Term,
		}

		if _, exists := seen[key]; exists {
			return fmt.Errorf(
				"duplicate read-state identity: request=%d term=%d",
				state.RequestID,
				state.Term,
			)
		}

		seen[key] = struct{}{}
	}

	for _, state := range states {
		if !d.readBarrierSatisfied(state.Index) {
			continue
		}

		delivered := state

		delivered.Context = append(
			[]byte(nil),
			state.Context...,
		)

		if err := d.readStateHandler(delivered); err != nil {
			return fmt.Errorf(
				"deliver read state %d: %w",
				state.RequestID,
				err,
			)
		}

		advancer.AdvanceReadStates(
			[]raft.ReadState{state},
		)
	}

	return nil
}
