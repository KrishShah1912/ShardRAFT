package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type Store struct {
	mu sync.RWMutex

	data   map[string]string
	dedup  map[RequestKey]RequestRecord
	engine Engine

	appliedIndex uint64
}

func New() *Store {
	return &Store{
		data:  make(map[string]string),
		dedup: make(map[RequestKey]RequestRecord),
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
			s.appliedIndex+1, entry.Index,
		)
	}

	switch entry.Type {
	case raft.EntryNoop:
		// No-op advances the applied index only.

	case raft.EntryNormal:
		command, err := DecodeCommand(entry.Data)
		if err != nil {
			return fmt.Errorf("apply entry %d: %w", entry.Index, err)
		}

		key := RequestKey{
			ClientID:  command.ClientID,
			RequestID: command.RequestID,
		}

		if command.ClientID != "" {
			if _, exists := s.dedup[key]; exists {
				// Already committed: preserve the original result.
				// Advance the applied index even for conflicting
				// commands, so Raft replay remains deterministic.
				break
			}
		}

		result := RequestResult{}
		write := false
		remove := false

		switch command.Op {
		case OpSet, OpPut:
			write = true
			result.Applied = true
			result.Value = command.Value

		case OpDelete:
			_, result.Applied = s.data[command.Key]
			remove = true

		case OpCompareAndSwap:
			current, exists := s.data[command.Key]
			if exists && current == command.Expected {
				write = true
				result.Applied = true
				result.Value = command.Value
			}
		}

		// Persist the materialized view before advancing
		// the in-memory state or applied index.
		if s.engine != nil {
			if write {
				if err := s.engine.Put(
					command.Key, command.Value,
				); err != nil {
					return fmt.Errorf(
						"persist KV entry %d: %w",
						entry.Index, err,
					)
				}
			} else if remove {
				if err := s.engine.Delete(command.Key); err != nil {
					return fmt.Errorf(
						"persist KV deletion %d: %w",
						entry.Index, err,
					)
				}
			}
		}

		if write {
			s.data[command.Key] = command.Value
		} else if remove {
			delete(s.data, command.Key)
		}

		if command.ClientID != "" {
			s.dedup[key] = RequestRecord{
				Command: command,
				Result:  result,
			}
		}

	default:
		return fmt.Errorf(
			"unsupported entry type: %d", entry.Type,
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

// RequestResult returns the original result of an
// already applied client request.
func (s *Store) RequestResult(
	clientID, requestID string,
) (RequestResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, exists := s.dedup[RequestKey{
		ClientID:  clientID,
		RequestID: requestID,
	}]
	return record.Result, exists
}

// DedupSnapshot returns an independent copy of
// the request history for snapshot persistence.
func (s *Store) DedupSnapshot() map[string]RequestRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]RequestRecord, len(s.dedup))

	for key, record := range s.dedup {
		encoded, err := json.Marshal(key)
		if err != nil {
			panic(err) // RequestKey contains only strings.
		}
		result[string(encoded)] = record
	}

	return result
}
