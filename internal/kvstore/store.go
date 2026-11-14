package kvstore

import (
	"errors"
	"fmt"
	"sync"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type Store struct {
	mu sync.RWMutex

	data map[string]string

	appliedIndex uint64
}

func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Apply processes a committed Raft entry.
//
// Entries must be applied in index order.
// Replaying an already applied entry is a no-op.
func (s *Store) Apply(entry raft.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.Index == 0 {
		return errors.New("entry index cannot be zero")
	}

	if entry.Index <= s.appliedIndex {
		return nil
	}

	if entry.Index != s.appliedIndex+1 {
		return fmt.Errorf(
			"out-of-order entry: expected %d, got %d",
			s.appliedIndex+1,
			entry.Index,
		)
	}

	switch entry.Type {
	case raft.EntryNoop:
		// No-op entries advance the applied index
		// without modifying the key-value data.

	case raft.EntryNormal:
		command, err := DecodeCommand(entry.Data)
		if err != nil {
			return fmt.Errorf(
				"apply entry %d: %w",
				entry.Index,
				err,
			)
		}

		switch command.Op {
		case OpSet:
			s.data[command.Key] = command.Value

		case OpDelete:
			delete(s.data, command.Key)
		}

	default:
		return fmt.Errorf(
			"unsupported entry type: %d",
			entry.Type,
		)
	}

	s.appliedIndex = entry.Index

	return nil
}

// Get performs a local read of the applied state.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	return value, exists
}

// AppliedIndex returns the last applied Raft index.
func (s *Store) AppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.appliedIndex
}

// Snapshot returns a copy of the current data.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string, len(s.data))

	for key, value := range s.data {
		result[key] = value
	}

	return result
}

/*
Restore replaces the in-memory KV state with
a previously saved snapshot.

Call Restore before processing new Raft entries.
*/
func (s *Store) Restore(
	appliedIndex uint64,
	data map[string]string,
) error {
	if data == nil {
		return errors.New("snapshot data cannot be nil")
	}

	copied := make(map[string]string, len(data))

	for key, value := range data {
		copied[key] = value
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Restoration is only supported on a fresh store.
	if s.appliedIndex != 0 || len(s.data) != 0 {
		return errors.New(
			"cannot restore into a nonempty KV store",
		)
	}

	s.data = copied
	s.appliedIndex = appliedIndex

	return nil
}
