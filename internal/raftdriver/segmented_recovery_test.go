package raftdriver

import (
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

func TestRecoverSegmentedDriver(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	options := wal.SegmentOptions{
		MaxSegmentSize: 256,
	}

	send := func([]raft.Message) error {
		return nil
	}

	apply := func(raft.Entry) error {
		return nil
	}

	node, storage, driver, err := RecoverSegmentedDriver(
		path,
		options,
		1,
		[]raft.NodeID{1},
		5,
		apply,
		send,
	)
	if err != nil {
		t.Fatal(err)
	}

	if node == nil || storage == nil || driver == nil {
		t.Fatal("expected initialized Raft driver")
	}

	if err := storage.SaveHardState(raft.HardState{
		Term: 3,
	}); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	recoveredNode, recoveredStorage, recoveredDriver, err :=
		RecoverSegmentedDriver(
			path,
			options,
			1,
			[]raft.NodeID{1},
			5,
			apply,
			send,
		)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredStorage.Close()

	if recoveredNode == nil || recoveredDriver == nil {
		t.Fatal("expected recovered Raft driver")
	}

	if recoveredNode.Status().Term != 3 {
		t.Fatalf(
			"recovered term = %d, want 3",
			recoveredNode.Status().Term,
		)
	}
}
