package meta

import (
	"errors"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// shardMapAtVersion returns the default layout stamped with a version.
func shardMapAtVersion(version uint64) ShardMap {
	m := DefaultShardMap()
	m.Version = version
	return m
}

func setCommand(expected uint64, m ShardMap) Command {
	return Command{
		Type:            SetShardMap,
		ExpectedVersion: expected,
		ShardMap:        m,
	}
}

func TestStateMachineStartsEmpty(t *testing.T) {
	sm := NewStateMachine()

	if sm.Version() != 0 {
		t.Fatalf("initial version = %d, want 0", sm.Version())
	}

	if len(sm.Current().Shards) != 0 {
		t.Fatal("initial shard map is not empty")
	}
}

func TestStateMachineAppliesSetShardMap(t *testing.T) {
	sm := NewStateMachine()

	if err := sm.Apply(setCommand(0, shardMapAtVersion(1))); err != nil {
		t.Fatal(err)
	}

	if sm.Version() != 1 {
		t.Fatalf("version = %d, want 1", sm.Version())
	}

	// A second update to version 2 is accepted.
	next := shardMapAtVersion(2)
	next.Shards[0].Replicas = []raft.NodeID{1, 2, 3, 4}

	if err := sm.Apply(setCommand(1, next)); err != nil {
		t.Fatal(err)
	}

	if sm.Version() != 2 {
		t.Fatalf("version = %d, want 2", sm.Version())
	}

	if len(sm.Current().Shards[0].Replicas) != 4 {
		t.Fatal("second update was not applied")
	}
}

func TestStateMachineRejectsStaleExpectedVersion(t *testing.T) {
	sm := NewStateMachine()

	if err := sm.Apply(setCommand(0, shardMapAtVersion(1))); err != nil {
		t.Fatal(err)
	}

	before := sm.Current()

	// Expected version no longer matches the current version.
	err := sm.Apply(setCommand(0, shardMapAtVersion(2)))
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(sm.Current(), before) {
		t.Fatal("rejected command changed the state")
	}
}

func TestStateMachineRejectsNonConsecutiveVersion(t *testing.T) {
	sm := NewStateMachine()

	if err := sm.Apply(setCommand(0, shardMapAtVersion(1))); err != nil {
		t.Fatal(err)
	}

	before := sm.Current()

	// New version must be current plus one; 3 skips 2.
	err := sm.Apply(setCommand(1, shardMapAtVersion(3)))
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(sm.Current(), before) {
		t.Fatal("rejected command changed the state")
	}
}

func TestStateMachineRejectsInvalidMap(t *testing.T) {
	sm := NewStateMachine()

	invalid := shardMapAtVersion(1)
	invalid.Shards[0].Range.End = 16382 // gap

	err := sm.Apply(setCommand(0, invalid))
	if !errors.Is(err, ErrInvalidShardMap) {
		t.Fatalf("unexpected error: %v", err)
	}

	if sm.Version() != 0 || len(sm.Current().Shards) != 0 {
		t.Fatal("rejected command changed the state")
	}
}

func TestStateMachineRejectsUnknownCommand(t *testing.T) {
	sm := NewStateMachine()

	err := sm.Apply(Command{Type: "NOPE", ShardMap: shardMapAtVersion(1)})
	if !errors.Is(err, ErrUnknownCommand) {
		t.Fatalf("unexpected error: %v", err)
	}

	if sm.Version() != 0 {
		t.Fatal("unknown command changed the state")
	}
}

func TestStateMachineIsDeterministic(t *testing.T) {
	commands := []Command{
		setCommand(0, shardMapAtVersion(1)),
		setCommand(0, shardMapAtVersion(2)), // rejected: stale
		setCommand(1, shardMapAtVersion(2)),
		setCommand(2, shardMapAtVersion(3)),
	}

	a := NewStateMachine()
	b := NewStateMachine()

	for _, cmd := range commands {
		errA := a.Apply(cmd)
		errB := b.Apply(cmd)

		if (errA == nil) != (errB == nil) {
			t.Fatalf("divergent outcomes: %v vs %v", errA, errB)
		}
	}

	if !reflect.DeepEqual(a.Current(), b.Current()) {
		t.Fatal("identical command sequences produced different state")
	}

	if a.Version() != 3 {
		t.Fatalf("final version = %d, want 3", a.Version())
	}
}

func TestStateMachineSnapshotRestore(t *testing.T) {
	sm := NewStateMachine()

	if err := sm.Apply(setCommand(0, shardMapAtVersion(1))); err != nil {
		t.Fatal(err)
	}
	if err := sm.Apply(setCommand(1, shardMapAtVersion(2))); err != nil {
		t.Fatal(err)
	}

	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	restored := NewStateMachine()
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(restored.Current(), sm.Current()) {
		t.Fatal("restored state differs from snapshot source")
	}

	// A restored state machine continues the version sequence.
	if err := restored.Apply(setCommand(2, shardMapAtVersion(3))); err != nil {
		t.Fatalf("restored state rejected the next update: %v", err)
	}
}

func TestStateMachineRestoreEmptySnapshot(t *testing.T) {
	sm := NewStateMachine()

	if err := sm.Restore(nil); err != nil {
		t.Fatal(err)
	}

	if sm.Version() != 0 || len(sm.Current().Shards) != 0 {
		t.Fatal("empty snapshot did not restore the initial state")
	}
}

func TestEncodeDecodeCommand(t *testing.T) {
	cmd := setCommand(0, shardMapAtVersion(1))

	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(decoded, cmd) {
		t.Fatal("round-tripped command differs from the original")
	}

	if _, err := EncodeCommand(Command{Type: "NOPE"}); !errors.Is(
		err,
		ErrUnknownCommand,
	) {
		t.Fatalf("unexpected error: %v", err)
	}
}
