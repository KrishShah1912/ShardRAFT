package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestPendingRequestRemovedAfterLeaderReplacement(t *testing.T) {
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

	original := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "account",
		Value:     "original",
		ClientID:  "client-1",
		RequestID: "request-1",
	}

	originalData, err := kvstore.EncodeCommand(original)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Step(raft.Message{
		Type:    raft.MsgAppend,
		From:    2,
		To:      1,
		Term:    1,
		Index:   0,
		LogTerm: 0,
		Commit:  0,
		Entries: []raft.Entry{{
			Index: 1,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  originalData,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	found, err := s.checkPendingRequest(original)
	if !found || !errors.Is(err, ErrRequestPending) {
		t.Fatalf("expected original pending request, found=%v err=%v", found, err)
	}

	replacement := kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "other",
		Value:     "replacement",
		ClientID:  "client-2",
		RequestID: "request-2",
	}

	replacementData, err := kvstore.EncodeCommand(replacement)
	if err != nil {
		t.Fatal(err)
	}

	// A new leader with a higher term replaces the
	// uncommitted entry at index 1.
	if err := s.Step(raft.Message{
		Type:    raft.MsgAppend,
		From:    3,
		To:      1,
		Term:    2,
		Index:   0,
		LogTerm: 0,
		Commit:  0,
		Entries: []raft.Entry{{
			Index: 1,
			Term:  2,
			Type:  raft.EntryNormal,
			Data:  replacementData,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	found, err = s.checkPendingRequest(original)
	if found || err != nil {
		t.Fatalf("replaced request still pending: found=%v err=%v", found, err)
	}

	found, err = s.checkPendingRequest(replacement)
	if !found || !errors.Is(err, ErrRequestPending) {
		t.Fatalf("replacement request missing: found=%v err=%v", found, err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify that the replacement also survives restart.
	recovered, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	found, err = recovered.checkPendingRequest(original)
	if found || err != nil {
		t.Fatalf("old request recovered: found=%v err=%v", found, err)
	}

	found, err = recovered.checkPendingRequest(replacement)
	if !found || !errors.Is(err, ErrRequestPending) {
		t.Fatalf("replacement not recovered: found=%v err=%v", found, err)
	}
}
