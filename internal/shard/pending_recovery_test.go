package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestPendingRequestSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")

	cfg := Config{
		ID:              1,
		Peers:           []raft.NodeID{2, 3},
		ElectionTimeout: 5,
		WALPath:         path,
		Send: func([]raft.Message) error {
			return nil
		},
	}

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	command := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "account",
		Value:     "original",
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	data, err := kvstore.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}

	msg := raft.Message{
		Type:    raft.MsgAppend,
		From:    2,
		To:      1,
		Term:    1,
		Index:   0,
		LogTerm: 0,
		Commit:  0,
		Entries: []raft.Entry{
			{
				Index: 1,
				Term:  1,
				Type:  raft.EntryNormal,
				Data:  data,
			},
		},
	}

	if err := s.Step(msg); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	found, err := recovered.checkPendingRequest(command)
	if !found || !errors.Is(err, ErrRequestPending) {
		t.Fatalf("recovered pending request: found=%v err=%v",
			found, err)
	}

	conflict := command
	conflict.Value = "different"

	found, err = recovered.checkPendingRequest(conflict)
	if !found || !errors.Is(err, kvstore.ErrRequestConflict) {
		t.Fatalf("recovered conflict: found=%v err=%v",
			found, err)
	}
}
