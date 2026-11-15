package kvstore

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

func TestDedupSurvivesSnapshotRoundTrip(t *testing.T) {
	original := New()

	commands := []Command{
		{Op: OpPut, Key: "counter", Value: "10"},
		{
			Op:  OpCompareAndSwap,
			Key: "counter", Expected: "10", Value: "20",
			ClientID: "client-1", RequestID: "request-1",
		},
	}

	for i, command := range commands {
		if err := original.Apply(
			makeEntry(t, uint64(i+1), command),
		); err != nil {
			t.Fatal(err)
		}
	}

	saved := snapshot.NewWithDedup(
		2, 1,
		original.Snapshot(),
		original.SnapshotDedup(),
	)

	restored := New()

	if err := restored.Restore(saved.AppliedIndex, saved.Data); err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreDedup(saved.Dedup); err != nil {
		t.Fatal(err)
	}

	result, found := restored.RequestResult(
		"client-1", "request-1",
	)
	if !found || !result.Applied || result.Value != "20" {
		t.Fatalf("dedup history not restored: %+v, found=%v",
			result, found)
	}

	// Retry the original CAS at a new committed log index.
	// It must not execute again.
	if err := restored.Apply(
		makeEntry(t, 3, commands[1]),
	); err != nil {
		t.Fatal(err)
	}

	value, _ := restored.Get("counter")
	if value != "20" {
		t.Fatalf("unexpected value after retry: %q", value)
	}
}
