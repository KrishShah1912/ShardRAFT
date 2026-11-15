package kvstore

import (
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type failingEngine struct {
	failWrites bool
}

func (e *failingEngine) Put(key, value string) error {
	if e.failWrites {
		return errors.New("injected Pebble write failure")
	}
	return nil
}

func (e *failingEngine) Get(key string) (string, bool, error) {
	return "", false, nil
}

func (e *failingEngine) Delete(key string) error {
	if e.failWrites {
		return errors.New("injected Pebble delete failure")
	}
	return nil
}

func (e *failingEngine) Clear() error {
	return nil
}

func (e *failingEngine) Close() error {
	return nil
}

func TestEngineFailureDoesNotAdvanceState(t *testing.T) {
	engine := &failingEngine{failWrites: true}

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

	entry := raft.Entry{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  command,
	}

	if err := store.Apply(entry); err == nil {
		t.Fatal("expected engine write failure")
	}

	if store.AppliedIndex() != 0 {
		t.Fatal("applied index advanced despite engine failure")
	}

	if _, exists := store.Get("alpha"); exists {
		t.Fatal("in-memory KV state changed despite engine failure")
	}

	engine.failWrites = false

	if err := store.Apply(entry); err != nil {
		t.Fatalf("retry after engine recovery failed: %v", err)
	}

	value, exists := store.Get("alpha")
	if !exists || value != "100" {
		t.Fatalf("expected alpha=100, got %q", value)
	}

	if store.AppliedIndex() != 1 {
		t.Fatal("applied index did not advance after successful retry")
	}
}
