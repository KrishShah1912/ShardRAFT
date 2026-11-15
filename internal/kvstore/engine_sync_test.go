package kvstore

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSyncEngineCopiesAppliedState(t *testing.T) {
	engine, err := OpenPebble(
		filepath.Join(t.TempDir(), "pebble"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	store, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}

	command, err := EncodeCommand(Command{
		Op: OpPut, Key: "alpha", Value: "100",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Apply(raft.Entry{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  command,
	}); err != nil {
		t.Fatal(err)
	}

	if err := store.SyncEngine(); err != nil {
		t.Fatal(err)
	}

	value, exists, err := engine.Get("alpha")
	if err != nil {
		t.Fatal(err)
	}

	if !exists || value != "100" {
		t.Fatalf(
			"expected Pebble alpha=100, got %q",
			value,
		)
	}
}
