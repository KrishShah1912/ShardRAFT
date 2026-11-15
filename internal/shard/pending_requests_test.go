package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func openPendingTestShard(t *testing.T) *Shard {
	t.Helper()

	s, err := Open(Config{
		ID:              1,
		Peers:           []raft.NodeID{2, 3},
		ElectionTimeout: 5,
		WALPath:         filepath.Join(t.TempDir(), "wal"),
		Send: func([]raft.Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestPendingRequestDetection(t *testing.T) {
	s := openPendingTestShard(t)

	original := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "account",
		Value:     "first",
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	data, err := kvstore.EncodeCommand(original)
	if err != nil {
		t.Fatal(err)
	}

	// Inject an uncommitted entry into the Raft log.
	// The node is not elected, so this test exercises
	// the pending-log lookup directly.
	s.node.Step(raft.Message{
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
	})

	found, err := s.checkPendingRequest(original)
	if !found || !errors.Is(err, ErrRequestPending) {
		t.Fatalf("identical pending request: found=%v err=%v",
			found, err)
	}

	conflict := original
	conflict.Value = "second"

	found, err = s.checkPendingRequest(conflict)
	if !found || !errors.Is(err, kvstore.ErrRequestConflict) {
		t.Fatalf("conflicting pending request: found=%v err=%v",
			found, err)
	}

	different := original
	different.RequestID = "request-2"

	found, err = s.checkPendingRequest(different)
	if found || err != nil {
		t.Fatalf("different request: found=%v err=%v",
			found, err)
	}
}
