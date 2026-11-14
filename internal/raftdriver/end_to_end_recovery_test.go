package raftdriver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

func TestEndToEndRecoveryWithPhysicalCompaction(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "node.wal")
	snapshotPath := filepath.Join(dir, "state.snapshot")

	send := func([]raft.Message) error {
		return nil
	}

	// Start a single-node Raft cluster.
	node, storage, driver, store, err := RecoverKVDriver(
		walPath, snapshotPath, 1, nil, 5, send,
	)
	if err != nil {
		t.Fatal(err)
	}

	elect := func(n *raft.Raft, d *Driver) {
		t.Helper()

		for i := 0; i < 5; i++ {
			n.Tick()
		}

		if err := d.Process(); err != nil {
			t.Fatal(err)
		}

		if n.Status().Role != raft.Leader {
			t.Fatal("expected single-node leader")
		}
	}

	propose := func(
		n *raft.Raft,
		d *Driver,
		key string,
		value string,
	) {
		t.Helper()

		data, err := kvstore.EncodeCommand(kvstore.Command{
			Op:    kvstore.OpSet,
			Key:   key,
			Value: value,
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := n.Propose(data); err != nil {
			t.Fatal(err)
		}

		if err := d.Process(); err != nil {
			t.Fatal(err)
		}
	}

	verify := func(
		s *kvstore.Store,
		expected map[string]string,
	) {
		t.Helper()

		for key, want := range expected {
			got, ok := s.Get(key)
			if !ok || got != want {
				t.Fatalf(
					"key %q: expected %q, got %q (exists=%v)",
					key, want, got, ok,
				)
			}
		}
	}

	elect(node, driver)

	propose(node, driver, "alpha", "10")
	propose(node, driver, "beta", "20")

	verify(store, map[string]string{
		"alpha": "10",
		"beta":  "20",
	})

	// Save a durable KV snapshot at the current
	// applied index.
	snapshotIndex := store.AppliedIndex()

	snapshotTerm, ok := node.TermAt(snapshotIndex)
	if !ok {
		t.Fatal("snapshot term unavailable")
	}

	saved := snapshot.New(
		snapshotIndex,
		snapshotTerm,
		store.Snapshot(),
	)

	if err := snapshot.Save(snapshotPath, saved); err != nil {
		t.Fatal(err)
	}

	verified, err := snapshot.Load(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}

	if verified.AppliedIndex != snapshotIndex ||
		verified.AppliedTerm != snapshotTerm {
		t.Fatal("saved snapshot boundary mismatch")
	}

	// Write a command after the snapshot.
	// Recovery must replay this command from WAL.
	propose(node, driver, "gamma", "30")

	expectedApplied := store.AppliedIndex()

	// Logically compact the persisted WAL.
	if err := storage.CompactPrefix(
		snapshotIndex,
		snapshotTerm,
	); err != nil {
		t.Fatal(err)
	}

	// Physically rewrite the WAL as a checkpoint.
	if err := storage.RewriteCheckpoint(walPath); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart 1: restore snapshot and replay the
	// committed command after the snapshot.
	node, storage, driver, store, err = RecoverKVDriver(
		walPath, snapshotPath, 1, nil, 5, send,
	)
	if err != nil {
		t.Fatal(err)
	}

	if node.Status().Role != raft.Follower {
		t.Fatal("recovered node should start as follower")
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	verify(store, map[string]string{
		"alpha": "10",
		"beta":  "20",
		"gamma": "30",
	})

	if store.AppliedIndex() != expectedApplied {
		t.Fatalf(
			"first recovery applied index: expected %d, got %d",
			expectedApplied,
			store.AppliedIndex(),
		)
	}

	// The recovered node must be able to elect
	// itself and accept another command.
	elect(node, driver)

	propose(node, driver, "delta", "40")

	verify(store, map[string]string{
		"alpha": "10",
		"beta":  "20",
		"gamma": "30",
		"delta": "40",
	})

	expectedApplied = store.AppliedIndex()

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart 2: verify that all four commands
	// survive another complete recovery.
	node, storage, driver, store, err = RecoverKVDriver(
		walPath, snapshotPath, 1, nil, 5, send,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	if node.Status().Role != raft.Follower {
		t.Fatal("second recovery should start as follower")
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	verify(store, map[string]string{
		"alpha": "10",
		"beta":  "20",
		"gamma": "30",
		"delta": "40",
	})

	if store.AppliedIndex() != expectedApplied {
		t.Fatalf(
			"second recovery applied index: expected %d, got %d",
			expectedApplied,
			store.AppliedIndex(),
		)
	}
}

// Verify that a corrupt WAL prevents Raft recovery.
func TestEndToEndRecoveryRejectsCorruptWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	storage, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := storage.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("command")},
	}); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Append an incomplete record header.
	f, err := os.OpenFile(
		path,
		os.O_WRONLY|os.O_APPEND,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		f.Close()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	node, reopened, err := RecoverNode(
		path, 1, nil, 5,
	)

	if err == nil {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatal("expected recovery failure")
	}

	if node != nil || reopened != nil {
		t.Fatal("corrupt WAL must not return a restored node")
	}
}
