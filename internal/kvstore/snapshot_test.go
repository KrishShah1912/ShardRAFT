package kvstore

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestRestoreSnapshot(t *testing.T) {
	store := New()

	s := snapshot.New(
		5,
		2,
		map[string]string{
			"x":    "10",
			"name": "ShardRAFT",
		},
	)

	if err := store.Restore(s.AppliedIndex, s.Data); err != nil {
		t.Fatal(err)
	}

	if store.AppliedIndex() != 5 {
		t.Fatalf(
			"expected applied index 5, got %d",
			store.AppliedIndex(),
		)
	}

	value, exists := store.Get("name")
	if !exists || value != "ShardRAFT" {
		t.Fatal("snapshot value not restored")
	}
}

func TestRestoreThenApplyNewEntry(t *testing.T) {
	store := New()

	s := snapshot.New(
		2,
		1,
		map[string]string{"x": "10"},
	)

	if err := store.Restore(s.AppliedIndex, s.Data); err != nil {
		t.Fatal(err)
	}

	data, err := EncodeCommand(Command{
		Op:    OpSet,
		Key:   "x",
		Value: "20",
	})
	if err != nil {
		t.Fatal(err)
	}

	entry := raft.Entry{
		Index: 3,
		Term:  2,
		Type:  raft.EntryNormal,
		Data:  data,
	}

	if err := store.Apply(entry); err != nil {
		t.Fatal(err)
	}

	value, _ := store.Get("x")

	if value != "20" {
		t.Fatalf("expected 20, got %q", value)
	}
}

func TestRestoreRejectsNonemptyStore(t *testing.T) {
	store := New()

	entry := raft.Entry{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNoop,
	}

	if err := store.Apply(entry); err != nil {
		t.Fatal(err)
	}

	err := store.Restore(
		5,
		map[string]string{"x": "10"},
	)

	if err == nil {
		t.Fatal("expected restore error")
	}
}

func TestRestoreCopiesData(t *testing.T) {
	store := New()

	data := map[string]string{"x": "10"}

	if err := store.Restore(3, data); err != nil {
		t.Fatal(err)
	}

	data["x"] = "999"

	value, _ := store.Get("x")

	if value != "10" {
		t.Fatal("restored store shares mutable data")
	}
}
