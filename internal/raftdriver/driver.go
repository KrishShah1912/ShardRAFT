package raftdriver

import (
	"errors"
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

// Storage is the persistence interface required by the driver.
type Storage interface {
	AppendEntries(entries []raft.Entry) error
	ReplaceEntries(fromIndex uint64, entries []raft.Entry) error
	TruncateSuffix(fromIndex uint64) error
	SaveHardState(state raft.HardState) error
	Sync() error
}

// Verify that WAL implements Storage.
var _ Storage = (*wal.WAL)(nil)
var _ Storage = (*wal.SegmentedWAL)(nil)

// Consensus is the Raft interface required by the driver.
type Consensus interface {
	Ready() raft.Ready
	Advance()
}

// ApplyFunc applies one committed entry to a state machine.
type ApplyFunc func(entry raft.Entry) error

// SendFunc sends a batch of Raft messages.
type SendFunc func(messages []raft.Message) error

var ErrStopped = errors.New("raft driver stopped")

// Driver coordinates Raft persistence, application,
// and outgoing message delivery.
type Driver struct {
	node    Consensus
	storage Storage
	apply   ApplyFunc
	send    SendFunc

	stopped bool
}

// New creates a WAL-backed Raft driver.
//
// Call Process() after Tick(), Step(), or Propose()
// produces new Raft work.
func New(
	node Consensus,
	storage Storage,
	apply ApplyFunc,
	send SendFunc,
) (*Driver, error) {
	if node == nil {
		return nil, errors.New("nil Raft node")
	}

	if storage == nil {
		return nil, errors.New("nil storage")
	}

	if apply == nil {
		return nil, errors.New("nil apply callback")
	}

	if send == nil {
		return nil, errors.New("nil send callback")
	}

	return &Driver{
		node:    node,
		storage: storage,
		apply:   apply,
		send:    send,
	}, nil
}

// Process persists and applies one Ready batch.
//
// Required order:
// 1. Persist log entries/truncation.
// 2. Persist HardState.
// 3. Sync storage.
// 4. Apply committed entries.
// 5. Acknowledge Ready.
// 6. Send outgoing messages.
//
// The caller must serialize calls to Process() and
// all mutations of the underlying Raft node.
func (d *Driver) Process() error {
	if d.stopped {
		return ErrStopped
	}

	ready := d.node.Ready()

	// Step 1: Persist log modifications.
	if ready.TruncateFrom != 0 {
		if len(ready.Entries) > 0 {
			if err := d.storage.ReplaceEntries(
				ready.TruncateFrom,
				ready.Entries,
			); err != nil {
				return d.stop(fmt.Errorf(
					"replace log entries: %w", err,
				))
			}
		} else {
			if err := d.storage.TruncateSuffix(
				ready.TruncateFrom,
			); err != nil {
				return d.stop(fmt.Errorf(
					"truncate log suffix: %w", err,
				))
			}
		}
	} else if len(ready.Entries) > 0 {
		if err := d.storage.AppendEntries(
			ready.Entries,
		); err != nil {
			return d.stop(fmt.Errorf(
				"append log entries: %w", err,
			))
		}
	}

	// Step 2: Persist election and commit metadata.
	if ready.HardState != nil {
		if err := d.storage.SaveHardState(
			*ready.HardState,
		); err != nil {
			return d.stop(fmt.Errorf(
				"save hard state: %w", err,
			))
		}
	}

	// Step 3: Synchronize before applying or sending.
	if err := d.storage.Sync(); err != nil {
		return d.stop(fmt.Errorf(
			"sync storage: %w", err,
		))
	}

	// Step 4: Apply committed entries.
	for _, entry := range ready.CommittedEntries {
		if err := d.apply(entry); err != nil {
			return d.stop(fmt.Errorf(
				"apply committed entry %d: %w",
				entry.Index, err,
			))
		}
	}

	// Step 5: Acknowledge the completed Ready batch.
	d.node.Advance()

	// Step 6: Release messages after persistence.
	if len(ready.Messages) > 0 {
		if err := d.send(ready.Messages); err != nil {
			return d.stop(fmt.Errorf(
				"send Raft messages: %w", err,
			))
		}
	}

	return nil
}

func (d *Driver) stop(err error) error {
	d.stopped = true
	return err
}

// Stopped reports whether processing was disabled
// after an unrecoverable error.
func (d *Driver) Stopped() bool {
	return d.stopped
}
