package raftdriver

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestKVStoreWALRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv-node.wal")

	// First startup.
	store := kvstore.New()

	node, storage, driver, err := RecoverDriver(
		path,
		1,
		nil,
		5,
		store.Apply,
		func(messages []raft.Message) error {
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Elect a single-node leader.
	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != raft.Leader {
		t.Fatal("expected leader")
	}

	// Encode and propose SET command.
	command, err := kvstore.EncodeCommand(
		kvstore.Command{
			Op:    kvstore.OpSet,
			Key:   "name",
			Value: "ShardRAFT",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.Propose(command); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	// Verify the applied value.
	value, exists := store.Get("name")
	if !exists || value != "ShardRAFT" {
		t.Fatalf("unexpected value: %q", value)
	}

	if store.AppliedIndex() != 2 {
		t.Fatalf(
			"expected applied index 2, got %d",
			store.AppliedIndex(),
		)
	}

	// Clean shutdown.
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart with a fresh in-memory KV store.
	restoredStore := kvstore.New()

	restoredNode, reopened, restoredDriver, err := RecoverDriver(
		path,
		1,
		nil,
		5,
		restoredStore.Apply,
		func(messages []raft.Message) error {
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if restoredNode.Status().Role != raft.Follower {
		t.Fatal("recovered node must start as follower")
	}

	// Replay committed WAL entries.
	if err := restoredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	// Verify state survived restart through replay.
	value, exists = restoredStore.Get("name")
	if !exists || value != "ShardRAFT" {
		t.Fatalf(
			"recovered value mismatch: %q (exists=%v)",
			value,
			exists,
		)
	}

	if restoredStore.AppliedIndex() != 2 {
		t.Fatalf(
			"expected recovered applied index 2, got %d",
			restoredStore.AppliedIndex(),
		)
	}

	// Processing again should not reapply entries.
	if err := restoredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	if restoredStore.AppliedIndex() != 2 {
		t.Fatal("unexpected duplicate application")
	}
}
