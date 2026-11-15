package raftdriver

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestPebbleRecoveryRebuildsMaterializedView(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "node.wal")
	snapPath := filepath.Join(dir, "state.snapshot")
	pebblePath := filepath.Join(dir, "pebble")

	send := func([]raft.Message) error { return nil }

	node, storage, driver, store, engine, err :=
		RecoverPebbleKVDriver(
			walPath, snapPath, pebblePath,
			1, nil, 5, send,
		)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		node.Tick()
	}
	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	propose := func(command kvstore.Command) {
		t.Helper()
		data, err := kvstore.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Propose(data); err != nil {
			t.Fatal(err)
		}
		if err := driver.Process(); err != nil {
			t.Fatal(err)
		}
	}

	propose(kvstore.Command{
		Op: kvstore.OpPut, Key: "counter", Value: "10",
	})

	propose(kvstore.Command{
		Op:  kvstore.OpCompareAndSwap,
		Key: "counter", Expected: "10", Value: "20",
		ClientID: "client-1", RequestID: "request-1",
	})

	// Snapshot the state at counter=20.
	index, data, dedup := store.CaptureSnapshot()
	term, ok := node.TermAt(index)
	if !ok {
		t.Fatal("missing snapshot term")
	}
	if err := snapshot.Save(
		snapPath,
		snapshot.NewWithDedup(index, term, data, dedup),
	); err != nil {
		t.Fatal(err)
	}

	// This committed entry must be replayed after restart.
	propose(kvstore.Command{
		Op: kvstore.OpPut, Key: "after", Value: "snapshot",
	})

	if err := engine.Put("stale", "invalid"); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	_, reopened, recoveredDriver, recoveredStore, recoveredEngine, err :=
		RecoverPebbleKVDriver(
			walPath, snapPath, pebblePath,
			1, nil, 5, send,
		)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	defer recoveredEngine.Close()

	// The wrapper rebuilds Pebble from the snapshot.
	// Process() replays entries committed after it.
	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	for key, expected := range map[string]string{
		"counter": "20",
		"after":   "snapshot",
	} {
		value, exists, err := recoveredEngine.Get(key)
		if err != nil || !exists || value != expected {
			t.Fatalf(
				"Pebble %s: value=%q exists=%v err=%v",
				key, value, exists, err,
			)
		}

		memoryValue, memoryExists := recoveredStore.Get(key)
		if !memoryExists || memoryValue != expected {
			t.Fatalf("memory %s mismatch", key)
		}
	}

	if _, exists, err := recoveredEngine.Get("stale"); err != nil || exists {
		t.Fatalf("stale key survived: exists=%v err=%v", exists, err)
	}

	result, found := recoveredStore.RequestResult(
		"client-1", "request-1",
	)
	if !found || !result.Applied || result.Value != "20" {
		t.Fatalf("dedup result lost: %+v", result)
	}
}
