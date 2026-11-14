package raftdriver

import (
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

// RecoverNode opens a WAL and restores a Raft node.
//
// The returned node starts as a follower.
// Its committed entries are pending replay.
//
// The caller owns the returned WAL and must close it.
func RecoverNode(
	path string,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
) (*raft.Raft, *wal.WAL, error) {
	if electionTimeout <= 0 {
		return nil, nil, fmt.Errorf(
			"election timeout must be positive",
		)
	}

	storage, err := wal.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"open WAL: %w", err,
		)
	}

	state, err := storage.Recover()
	if err != nil {
		storage.Close()
		return nil, nil, fmt.Errorf(
			"recover WAL: %w", err,
		)
	}

	node, err := raft.NewRaftFromState(
		id,
		peers,
		electionTimeout,
		state,
	)
	if err != nil {
		storage.Close()
		return nil, nil, fmt.Errorf(
			"restore Raft state: %w", err,
		)
	}

	return node, storage, nil
}

// RecoverDriver restores Raft from its WAL and
// constructs a driver ready to replay committed entries.
//
// The caller must call driver.Process() successfully
// before allowing the node to receive new messages,
// ticks, or proposals.
//
// The caller owns the returned WAL and must close it.
func RecoverDriver(
	path string,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
	apply ApplyFunc,
	send SendFunc,
) (*raft.Raft, *wal.WAL, *Driver, error) {
	if apply == nil || send == nil {
		return nil, nil, nil, fmt.Errorf(
			"apply and send callbacks are required",
		)
	}

	node, storage, err := RecoverNode(
		path,
		id,
		peers,
		electionTimeout,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	driver, err := New(node, storage, apply, send)
	if err != nil {
		storage.Close()
		return nil, nil, nil, err
	}

	return node, storage, driver, nil
}
