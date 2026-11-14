package raftdriver

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

type fakeNode struct {
	ready    raft.Ready
	advanced bool
}

func (n *fakeNode) Ready() raft.Ready {
	return n.ready
}

func (n *fakeNode) Advance() {
	n.advanced = true
}

type fakeStorage struct {
	events []string

	appendErr error
	syncErr   error
}

func (s *fakeStorage) AppendEntries(
	entries []raft.Entry,
) error {
	s.events = append(s.events, "append")
	return s.appendErr
}

func (s *fakeStorage) ReplaceEntries(
	fromIndex uint64,
	entries []raft.Entry,
) error {
	s.events = append(s.events, "replace")
	return nil
}

func (s *fakeStorage) TruncateSuffix(
	fromIndex uint64,
) error {
	s.events = append(s.events, "truncate")
	return nil
}

func (s *fakeStorage) SaveHardState(
	state raft.HardState,
) error {
	s.events = append(s.events, "hardstate")
	return nil
}

func (s *fakeStorage) Sync() error {
	s.events = append(s.events, "sync")
	return s.syncErr
}

func TestDriverPersistsBeforeSending(t *testing.T) {
	node := &fakeNode{
		ready: raft.Ready{
			Entries: []raft.Entry{
				{Index: 1, Term: 1},
			},
			HardState: &raft.HardState{
				Term:   1,
				Commit: 1,
			},
			CommittedEntries: []raft.Entry{
				{Index: 1, Term: 1},
			},
			Messages: []raft.Message{
				{Type: raft.MsgAppend, From: 1, To: 2},
			},
		},
	}

	storage := &fakeStorage{}

	applied := false
	sent := false

	driver, err := New(
		node,
		storage,
		func(entry raft.Entry) error {
			applied = true
			storage.events = append(
				storage.events, "apply",
			)
			return nil
		},
		func(messages []raft.Message) error {
			sent = true
			storage.events = append(
				storage.events, "send",
			)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"append",
		"hardstate",
		"sync",
		"apply",
		"send",
	}

	if len(storage.events) != len(expected) {
		t.Fatalf(
			"unexpected events: %v",
			storage.events,
		)
	}

	for i, event := range expected {
		if storage.events[i] != event {
			t.Fatalf(
				"event %d: expected %s, got %s",
				i, event, storage.events[i],
			)
		}
	}

	if !node.advanced {
		t.Fatal("expected Advance to be called")
	}

	if !applied || !sent {
		t.Fatal("expected apply and send callbacks")
	}
}

func TestDriverDoesNotSendOnPersistenceFailure(
	t *testing.T,
) {
	node := &fakeNode{
		ready: raft.Ready{
			Entries: []raft.Entry{
				{Index: 1, Term: 1},
			},
			Messages: []raft.Message{
				{Type: raft.MsgAppend, From: 1, To: 2},
			},
		},
	}

	storage := &fakeStorage{
		appendErr: errors.New("disk failure"),
	}

	sent := false

	driver, err := New(
		node,
		storage,
		func(entry raft.Entry) error {
			return nil
		},
		func(messages []raft.Message) error {
			sent = true
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected persistence failure")
	}

	if sent {
		t.Fatal("messages sent despite persistence failure")
	}

	if node.advanced {
		t.Fatal("Advance called despite persistence failure")
	}

	if !driver.Stopped() {
		t.Fatal("driver should stop after persistence failure")
	}
}

func TestDriverDoesNotSendOnSyncFailure(t *testing.T) {
	node := &fakeNode{
		ready: raft.Ready{
			Messages: []raft.Message{
				{Type: raft.MsgVoteResp, From: 2, To: 1},
			},
		},
	}

	storage := &fakeStorage{
		syncErr: errors.New("fsync failure"),
	}

	sent := false

	driver, err := New(
		node,
		storage,
		func(entry raft.Entry) error {
			return nil
		},
		func(messages []raft.Message) error {
			sent = true
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected sync failure")
	}

	if sent || node.advanced {
		t.Fatal("driver released work after sync failure")
	}
}

func TestDriverWithRealWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")

	storage, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	node := raft.NewRaft(1, nil, 5)

	for i := 0; i < 5; i++ {
		node.Tick()
	}

	applied := 0

	driver, err := New(
		node,
		storage,
		func(entry raft.Entry) error {
			applied++
			return nil
		},
		func(messages []raft.Message) error {
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if applied != 1 {
		t.Fatalf(
			"expected one applied no-op, got %d",
			applied,
		)
	}

	if err := node.Propose([]byte("SET x=10")); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if applied != 2 {
		t.Fatalf(
			"expected two applied entries, got %d",
			applied,
		)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	state, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Log) != 2 {
		t.Fatalf(
			"expected 2 recovered entries, got %d",
			len(state.Log),
		)
	}

	if state.HardState.Commit != 2 {
		t.Fatalf(
			"expected recovered commit index 2, got %d",
			state.HardState.Commit,
		)
	}

	if string(state.Log[1].Data) != "SET x=10" {
		t.Fatal("incorrect recovered command")
	}
}

func TestDriverRejectsProcessingAfterFailure(t *testing.T) {
	node := &fakeNode{
		ready: raft.Ready{
			Entries: []raft.Entry{
				{Index: 1, Term: 1},
			},
		},
	}

	storage := &fakeStorage{
		appendErr: errors.New("disk unavailable"),
	}

	driver, err := New(
		node,
		storage,
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

	if err := driver.Process(); err == nil {
		t.Fatal("expected initial failure")
	}

	if err := driver.Process(); !errors.Is(err, ErrStopped) {
		t.Fatalf(
			"expected ErrStopped, got %v",
			err,
		)
	}
}
