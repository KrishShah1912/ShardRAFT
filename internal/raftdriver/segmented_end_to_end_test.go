package raftdriver

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

func TestSegmentedRaftEndToEndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	options := wal.SegmentOptions{
		MaxSegmentSize: 256,
	}

	send := func([]raft.Message) error {
		return nil
	}

	var initiallyApplied []string

	apply := func(entry raft.Entry) error {
		if entry.Type == raft.EntryNormal {
			initiallyApplied = append(
				initiallyApplied,
				string(entry.Data),
			)
		}
		return nil
	}

	node, storage, driver, err := RecoverSegmentedDriver(
		path, options, 1, nil, 5, apply, send,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Elect the single-node leader.
	for i := 0; i < 5; i++ {
		node.Tick()
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != raft.Leader {
		t.Fatal("expected elected leader")
	}

	// Propose enough commands to force WAL rotation.
	const commandCount = 20

	for i := 1; i <= commandCount; i++ {
		command := fmt.Sprintf("SET key-%02d=value-%02d", i, i)

		if err := node.Propose([]byte(command)); err != nil {
			t.Fatal(err)
		}

		if err := driver.Process(); err != nil {
			t.Fatal(err)
		}
	}

	before := node.Status()

	if before.Commit != commandCount+1 {
		t.Fatalf(
			"commit = %d, want %d",
			before.Commit,
			commandCount+1,
		)
	}

	if len(initiallyApplied) != commandCount {
		t.Fatalf(
			"initially applied = %d, want %d",
			len(initiallyApplied),
			commandCount,
		)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart and recover all WAL segments.
	var replayed []string

	replayApply := func(entry raft.Entry) error {
		if entry.Type == raft.EntryNormal {
			replayed = append(replayed, string(entry.Data))
		}
		return nil
	}

	restored, reopened, recoveredDriver, err :=
		RecoverSegmentedDriver(
			path, options, 1, nil, 5, replayApply, send,
		)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	status := restored.Status()

	if status.Role != raft.Follower {
		t.Fatal("recovered node must start as follower")
	}

	if status.Term != before.Term {
		t.Fatalf(
			"recovered term = %d, want %d",
			status.Term,
			before.Term,
		)
	}

	if status.Commit != before.Commit {
		t.Fatalf(
			"recovered commit = %d, want %d",
			status.Commit,
			before.Commit,
		)
	}

	// Replay committed entries before serving requests.
	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(replayed) != commandCount {
		t.Fatalf(
			"replayed %d commands, want %d",
			len(replayed),
			commandCount,
		)
	}

	for i, command := range replayed {
		expected := fmt.Sprintf(
			"SET key-%02d=value-%02d", i+1, i+1,
		)

		if command != expected {
			t.Fatalf(
				"command %d = %q, want %q",
				i, command, expected,
			)
		}
	}

	// Repeated processing must not replay entries twice.
	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(replayed) != commandCount {
		t.Fatal("committed entries replayed twice")
	}
}
