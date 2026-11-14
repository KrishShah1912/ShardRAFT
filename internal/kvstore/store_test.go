package kvstore

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func makeEntry(
	t *testing.T,
	index uint64,
	command Command,
) raft.Entry {
	t.Helper()

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}

	return raft.Entry{
		Index: index,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  data,
	}
}

func TestSetAndGet(t *testing.T) {
	store := New()

	entry := makeEntry(t, 1, Command{
		Op:    OpSet,
		Key:   "name",
		Value: "ShardRAFT",
	})

	if err := store.Apply(entry); err != nil {
		t.Fatal(err)
	}

	value, exists := store.Get("name")

	if !exists || value != "ShardRAFT" {
		t.Fatalf(
			"expected ShardRAFT, got %q (exists=%v)",
			value,
			exists,
		)
	}
}

func TestDelete(t *testing.T) {
	store := New()

	entries := []raft.Entry{
		makeEntry(t, 1, Command{
			Op:    OpSet,
			Key:   "x",
			Value: "10",
		}),
		makeEntry(t, 2, Command{
			Op:  OpDelete,
			Key: "x",
		}),
	}

	for _, entry := range entries {
		if err := store.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}

	if _, exists := store.Get("x"); exists {
		t.Fatal("expected key to be deleted")
	}
}

func TestReplayIsIdempotent(t *testing.T) {
	store := New()

	entry := makeEntry(t, 1, Command{
		Op:    OpSet,
		Key:   "x",
		Value: "10",
	})

	if err := store.Apply(entry); err != nil {
		t.Fatal(err)
	}

	if err := store.Apply(entry); err != nil {
		t.Fatal(err)
	}

	if store.AppliedIndex() != 1 {
		t.Fatal("duplicate replay advanced applied index")
	}
}

func TestRejectOutOfOrderEntry(t *testing.T) {
	store := New()

	entry := makeEntry(t, 2, Command{
		Op:    OpSet,
		Key:   "x",
		Value: "10",
	})

	if err := store.Apply(entry); err == nil {
		t.Fatal("expected out-of-order error")
	}
}

func TestNoopEntry(t *testing.T) {
	store := New()

	entry := raft.Entry{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNoop,
	}

	if err := store.Apply(entry); err != nil {
		t.Fatal(err)
	}

	if store.AppliedIndex() != 1 {
		t.Fatal("no-op did not advance applied index")
	}
}

func TestCommandEncoding(t *testing.T) {
	original := Command{
		Op:    OpSet,
		Key:   "language",
		Value: "Go",
	}

	encoded, err := EncodeCommand(original)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded != original {
		t.Fatalf(
			"expected %+v, got %+v",
			original,
			decoded,
		)
	}
}

func TestRejectInvalidCommand(t *testing.T) {
	_, err := EncodeCommand(Command{
		Op:  "UNKNOWN",
		Key: "x",
	})

	if err == nil {
		t.Fatal("expected invalid command error")
	}
}

func TestSnapshotIsIndependent(t *testing.T) {
	store := New()

	entry := makeEntry(t, 1, Command{
		Op:    OpSet,
		Key:   "x",
		Value: "10",
	})

	if err := store.Apply(entry); err != nil {
		t.Fatal(err)
	}

	snapshot := store.Snapshot()
	snapshot["x"] = "999"

	value, _ := store.Get("x")

	if value != "10" {
		t.Fatal("snapshot modified live store")
	}
}
