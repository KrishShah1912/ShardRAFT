package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestRequestResultAfterCommitAndRestart(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "raft.wal")

	cfg := Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         walPath,
		Send: func([]raft.Message) error {
			return nil
		},
	}

	node, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
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

	command := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "language",
		Value:     "Go",
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	if err := node.Propose(command); err != nil {
		t.Fatal(err)
	}

	result, found, err := node.RequestResult(
		"client-1",
		"request-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("committed request result not found")
	}
	if !result.Applied || result.Value != "Go" {
		t.Fatalf("unexpected result: %+v", result)
	}

	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()

	result, found, err = restarted.RequestResult(
		"client-1",
		"request-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("request result missing after restart")
	}
	if !result.Applied || result.Value != "Go" {
		t.Fatalf("unexpected recovered result: %+v", result)
	}
}

func TestRequestResultUnknownAndClosed(t *testing.T) {
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

	_, found, err := node.RequestResult(
		"unknown-client",
		"unknown-request",
	)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("unexpected result for unknown request")
	}

	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, err = node.RequestResult("client", "request")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}
