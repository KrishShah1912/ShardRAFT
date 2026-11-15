package kvstore

import (
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestConflictingCommittedRequestPreservesOriginalState(t *testing.T) {
	store := New()

	original := Command{
		Op:        OpPut,
		Key:       "account",
		Value:     "first",
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	firstData, err := EncodeCommand(original)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Apply(raft.Entry{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  firstData,
	}); err != nil {
		t.Fatal(err)
	}

	conflict := original
	conflict.Value = "second"

	_, found, err := store.CheckRequest(conflict)
	if !found || !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("expected proposal conflict, got found=%v err=%v", found, err)
	}

	conflictingData, err := EncodeCommand(conflict)
	if err != nil {
		t.Fatal(err)
	}

	// A conflicting entry already committed by Raft must
	// not prevent deterministic state-machine replay.
	if err := store.Apply(raft.Entry{
		Index: 2,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  conflictingData,
	}); err != nil {
		t.Fatalf("committed entry blocked replay: %v", err)
	}

	if got := store.AppliedIndex(); got != 2 {
		t.Fatalf("applied index = %d, want 2", got)
	}

	value, exists := store.Get("account")
	if !exists || value != "first" {
		t.Fatalf("value = %q, exists=%v", value, exists)
	}
}

func TestCheckRequestDetectsIdenticalAndConflictingRetry(t *testing.T) {
	store := New()

	original := Command{
		Op:        OpPut,
		Key:       "key",
		Value:     "value",
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	data, err := EncodeCommand(original)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Apply(raft.Entry{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  data,
	}); err != nil {
		t.Fatal(err)
	}

	result, found, err := store.CheckRequest(original)
	if err != nil || !found || !result.Applied {
		t.Fatalf("identical retry: result=%+v found=%v err=%v",
			result, found, err)
	}

	conflict := original
	conflict.Value = "different"

	_, found, err = store.CheckRequest(conflict)
	if !found || !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflicting retry: found=%v err=%v", found, err)
	}
}
