package raftdriver

import (
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

// RecoverPebbleKVDriver recovers Raft and KV state with
// Pebble as a rebuildable materialized view.
//
// The caller owns the returned WAL and Pebble engine.
func RecoverPebbleKVDriver(
	walPath string,
	snapshotPath string,
	pebblePath string,
	id raft.NodeID,
	peers []raft.NodeID,
	electionTimeout int,
	send SendFunc,
) (
	*raft.Raft,
	*wal.WAL,
	*Driver,
	*kvstore.Store,
	*kvstore.PebbleEngine,
	error,
) {
	engine, err := kvstore.OpenPebble(pebblePath)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	node, storage, driver, store, err :=
		recoverKVDriverWithEngine(
			walPath, snapshotPath, id, peers,
			electionTimeout, send, engine,
		)
	if err != nil {
		_ = engine.Close()
		return nil, nil, nil, nil, nil, err
	}

	// Reconcile Pebble with the restored snapshot state.
	// Remaining committed entries are replayed by Process().
	if err := store.SyncEngine(); err != nil {
		_ = storage.Close()
		_ = engine.Close()
		return nil, nil, nil, nil, nil,
			fmt.Errorf("rebuild Pebble view: %w", err)
	}

	return node, storage, driver, store, engine, nil
}
