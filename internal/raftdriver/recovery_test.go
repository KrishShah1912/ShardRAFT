package raftdriver

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestRecoverDriverRestoresPersistedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	// First startup: empty WAL.
	node, storage, driver, err := RecoverDriver(
		path,
		1,
		nil,
		5,
		func(entry raft.Entry) error {
			return nil
		},
		func(messages []raft.Message) error {
			return nil
		},
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

	// Write two commands.
	commands := []string{
		"SET x=10",
		"SET y=20",
	}

	for _, command := range commands {
		if err := node.Propose([]byte(command)); err != nil {
			t.Fatal(err)
		}

		if err := driver.Process(); err != nil {
			t.Fatal(err)
		}
	}

	before := node.Status()

	if before.Commit != 3 {
		t.Fatalf(
			"expected commit index 3, got %d",
			before.Commit,
		)
	}

	// Simulate a clean shutdown.
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart from the same WAL.
	var replayed []string

	restored, reopened, recoveredDriver, err := RecoverDriver(
		path,
		1,
		nil,
		5,
		func(entry raft.Entry) error {
			if entry.Type == raft.EntryNormal {
				replayed = append(
					replayed,
					string(entry.Data),
				)
			}
			return nil
		},
		func(messages []raft.Message) error {
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	// A recovered leader must restart as follower.
	status := restored.Status()

	if status.Role != raft.Follower {
		t.Fatal("recovered node must start as follower")
	}

	if status.Term != before.Term {
		t.Fatalf(
			"expected term %d, got %d",
			before.Term, status.Term,
		)
	}

	if status.Commit != before.Commit {
		t.Fatalf(
			"expected commit %d, got %d",
			before.Commit, status.Commit,
		)
	}

	// Reapply committed entries before normal operation.
	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(replayed) != 2 {
		t.Fatalf(
			"expected 2 replayed commands, got %d",
			len(replayed),
		)
	}

	for i, expected := range commands {
		if replayed[i] != expected {
			t.Fatalf(
				"command %d: expected %q, got %q",
				i, expected, replayed[i],
			)
		}
	}

	// Replaying again must not duplicate commands
	// within the same running node.
	if err := recoveredDriver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(replayed) != 2 {
		t.Fatal("committed entries were applied twice")
	}
}

func TestRecoverNodeRejectsInvalidTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.wal")

	_, _, err := RecoverNode(
		path,
		1,
		nil,
		0,
	)

	if err == nil {
		t.Fatal("expected invalid timeout error")
	}
}

func TestRecoverDriverRequiresCallbacks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "callbacks.wal")

	_, _, _, err := RecoverDriver(
		path,
		1,
		nil,
		5,
		nil,
		nil,
	)

	if err == nil {
		t.Fatal("expected missing callback error")
	}
}
