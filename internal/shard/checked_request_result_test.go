package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestCheckedRequestResult(t *testing.T) {
	node, err := Open(Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         filepath.Join(t.TempDir(), "raft.wal"),
		Send: func([]raft.Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	command := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "language",
		Value:     "Go",
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	_, found, err := node.CheckedRequestResult(command)
	if err != nil || found {
		t.Fatalf("unexpected result before proposal: found=%v err=%v", found, err)
	}

	for i := 0; i < 20; i++ {
		if err := node.Tick(); err != nil {
			t.Fatal(err)
		}
		status, err := node.Status()
		if err != nil {
			t.Fatal(err)
		}
		if status.Role == raft.Leader {
			break
		}
	}

	if err := node.Propose(command); err != nil {
		t.Fatal(err)
	}

	result, found, err := node.CheckedRequestResult(command)
	if err != nil {
		t.Fatal(err)
	}
	if !found || !result.Applied || result.Value != "Go" {
		t.Fatalf("unexpected committed result: found=%v result=%+v", found, result)
	}

	conflict := command
	conflict.Value = "Python"

	_, found, err = node.CheckedRequestResult(conflict)
	if !found || !errors.Is(err, kvstore.ErrRequestConflict) {
		t.Fatalf("expected request conflict, found=%v err=%v", found, err)
	}
}
