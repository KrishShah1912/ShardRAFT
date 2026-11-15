package kvstore

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestSyncEngineRemovesStaleKeys(t *testing.T) {
	engine, err := OpenPebble(
		filepath.Join(t.TempDir(), "pebble"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	if err := engine.Put("stale", "old"); err != nil {
		t.Fatal(err)
	}

	store, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}

	command, err := EncodeCommand(Command{
		Op: OpPut, Key: "active", Value: "new",
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

	if _, exists, err := engine.Get("stale"); err != nil || exists {
		t.Fatalf("stale key survived synchronization: exists=%v, err=%v",
			exists, err)
	}

	value, exists, err := engine.Get("active")
	if err != nil || !exists || value != "new" {
		t.Fatalf("active key missing: value=%q, exists=%v, err=%v",
			value, exists, err)
	}
}
