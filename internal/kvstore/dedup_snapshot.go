package kvstore

import (
	"encoding/json"
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

// SnapshotDedup exports request history in the snapshot format.
func (s *Store) SnapshotDedup() map[string]snapshot.DedupRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]snapshot.DedupRecord, len(s.dedup))

	for key, record := range s.dedup {
		encoded, _ := json.Marshal(key)

		result[string(encoded)] = snapshot.DedupRecord{
			Operation: string(record.Command.Op),
			Key:       record.Command.Key,
			Value:     record.Command.Value,
			Expected:  record.Command.Expected,
			Applied:   record.Result.Applied,
			Result:    record.Result.Value,
		}
	}

	return result
}

// RestoreDedup restores request history after KV snapshot restoration.
func (s *Store) RestoreDedup(
	records map[string]snapshot.DedupRecord,
) error {
	restored := make(map[RequestKey]RequestRecord, len(records))

	for encoded, record := range records {
		var key RequestKey
		if err := json.Unmarshal([]byte(encoded), &key); err != nil {
			return fmt.Errorf("invalid dedup key: %w", err)
		}

		if key.ClientID == "" || key.RequestID == "" {
			return fmt.Errorf("empty client or request ID in snapshot")
		}

		command := Command{
			Op:        Operation(record.Operation),
			Key:       record.Key,
			Value:     record.Value,
			Expected:  record.Expected,
			ClientID:  key.ClientID,
			RequestID: key.RequestID,
		}

		if err := command.Validate(); err != nil {
			return fmt.Errorf("invalid dedup command: %w", err)
		}

		if _, exists := restored[key]; exists {
			return fmt.Errorf("duplicate request key in snapshot")
		}

		restored[key] = RequestRecord{
			Command: command,
			Result: RequestResult{
				Applied: record.Applied,
				Value:   record.Result,
			},
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.appliedIndex == 0 && len(records) > 0 {
		return fmt.Errorf("cannot restore request history before KV snapshot")
	}
	if len(s.dedup) != 0 {
		return fmt.Errorf("request history already initialized")
	}

	s.dedup = restored
	return nil
}
