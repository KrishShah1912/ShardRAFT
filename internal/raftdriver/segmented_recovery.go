package raftdriver

import (
	"errors"
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

// RecoverSegmentedNode restores a Raft node from segmented WAL.
//
// The caller owns the returned storage and must close it.
func RecoverSegmentedNode(
	path string,
	options wal.SegmentOptions,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
) (*raft.Raft, *wal.SegmentedWAL, error) {
	if electionTimeout <= 0 {
		return nil, nil, errors.New(
			"election timeout must be positive",
		)
	}

	storage, err := wal.OpenSegmented(path, options)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"open segmented WAL: %w", err,
		)
	}

	state, err := storage.Recover()
	if err != nil {
		_ = storage.Close()
		return nil, nil, fmt.Errorf(
			"recover segmented WAL: %w", err,
		)
	}

	node, err := raft.NewRaftFromState(
		id,
		peers,
		electionTimeout,
		state,
	)
	if err != nil {
		_ = storage.Close()
		return nil, nil, fmt.Errorf(
			"restore Raft state: %w", err,
		)
	}

	return node, storage, nil
}

// RecoverSegmentedDriver constructs a driver backed
// by checksummed, append-only WAL segments.
//
// Call driver.Process() before accepting new activity.
func RecoverSegmentedDriver(
	path string,
	options wal.SegmentOptions,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
	apply ApplyFunc,
	send SendFunc,
) (*raft.Raft, *wal.SegmentedWAL, *Driver, error) {
	if apply == nil || send == nil {
		return nil, nil, nil, errors.New(
			"apply and send callbacks are required",
		)
	}

	node, storage, err := RecoverSegmentedNode(
		path, options, id, peers, electionTimeout,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	driver, err := New(node, storage, apply, send)
	if err != nil {
		_ = storage.Close()
		return nil, nil, nil, err
	}

	return node, storage, driver, nil
}
