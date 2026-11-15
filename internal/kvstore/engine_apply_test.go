package kvstore

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestCommittedEntriesPersistToPebble(t *testing.T) {
	engine, err := OpenPebble(filepath.Join(t.TempDir(), "pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	store, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}

	apply := func(index uint64, command Command) {
		t.Helper()
		data, err := EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Apply(raft.Entry{
			Index: index,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  data,
		}); err != nil {
			t.Fatal(err)
		}
	}

	apply(1, Command{Op: OpPut, Key: "alpha", Value: "100"})

	value, exists, err := engine.Get("alpha")
	if err != nil || !exists || value != "100" {
		t.Fatalf("Pebble Put failed: %q, %v, %v", value, exists, err)
	}

	apply(2, Command{
		Op:  OpCompareAndSwap,
		Key: "alpha", Expected: "100", Value: "200",
		ClientID: "client-1", RequestID: "request-1",
	})

	value, exists, err = engine.Get("alpha")
	if err != nil || !exists || value != "200" {
		t.Fatalf("Pebble CAS failed: %q, %v, %v", value, exists, err)
	}

	apply(3, Command{Op: OpDelete, Key: "alpha"})

	_, exists, err = engine.Get("alpha")
	if err != nil || exists {
		t.Fatalf("Pebble Delete failed: exists=%v, err=%v", exists, err)
	}

	if store.AppliedIndex() != 3 {
		t.Fatalf("expected applied index 3, got %d",
			store.AppliedIndex())
	}
}
