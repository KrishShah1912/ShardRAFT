package raftdriver

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestDedupSurvivesCompactionAndRestart(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "node.wal")
	snapPath := filepath.Join(dir, "state.snapshot")

	send := func([]raft.Message) error { return nil }

	node, storage, driver, store, err := RecoverKVDriver(
		walPath, snapPath, 1, nil, 5, send,
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

	cas := kvstore.Command{
		Op:  kvstore.OpCompareAndSwap,
		Key: "counter", Expected: "10", Value: "20",
		ClientID: "client-1", RequestID: "request-1",
	}
	propose(cas)

	result, found := store.RequestResult(
		"client-1", "request-1",
	)
	if !found || !result.Applied {
		t.Fatal("original CAS was not recorded")
	}

	if err := driver.CompactKV(
		node, storage, store, snapPath,
	); err != nil {
		t.Fatal(err)
	}

	saved, err := snapshot.Load(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Dedup) != 1 {
		t.Fatalf("expected 1 dedup record, got %d", len(saved.Dedup))
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	recoveredNode, reopened, recoveredDriver, recoveredStore, err :=
		RecoverKVDriver(walPath, snapPath, 1, nil, 5, send)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	recoveredResult, found := recoveredStore.RequestResult(
		"client-1", "request-1",
	)
	if !found || !recoveredResult.Applied ||
		recoveredResult.Value != "20" {
		t.Fatalf("dedup result lost after restart: %+v",
			recoveredResult)
	}

	value, exists := recoveredStore.Get("counter")
	if !exists || value != "20" {
		t.Fatalf("KV state lost after restart: %q", value)
	}

	// Re-elect the recovered single-node Raft instance.
	for i := 0; i < 5; i++ {
		recoveredNode.Tick()
	}
	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	// Retry the exact same client request.
	retryData, err := kvstore.EncodeCommand(cas)
	if err != nil {
		t.Fatal(err)
	}
	if err := recoveredNode.Propose(retryData); err != nil {
		t.Fatal(err)
	}
	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	value, exists = recoveredStore.Get("counter")
	if !exists || value != "20" {
		t.Fatalf("duplicate CAS changed state: %q", value)
	}

	retryResult, found := recoveredStore.RequestResult(
		"client-1", "request-1",
	)
	if !found || !retryResult.Applied ||
		retryResult.Value != "20" {
		t.Fatalf("original CAS result changed: %+v", retryResult)
	}
}
