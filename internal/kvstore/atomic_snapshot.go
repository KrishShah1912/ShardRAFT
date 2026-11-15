package kvstore

import (
	"encoding/json"

	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

// CaptureSnapshot copies the applied index, KV data,
// and deduplication history under one read lock.
func (s *Store) CaptureSnapshot() (
	uint64,
	map[string]string,
	map[string]snapshot.DedupRecord,
) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	index := s.appliedIndex

	data := make(map[string]string, len(s.data))
	for key, value := range s.data {
		data[key] = value
	}

	dedup := make(
		map[string]snapshot.DedupRecord,
		len(s.dedup),
	)

	for key, record := range s.dedup {
		encoded, _ := json.Marshal(key)

		dedup[string(encoded)] = snapshot.DedupRecord{
			Operation: string(record.Command.Op),
			Key:       record.Command.Key,
			Value:     record.Command.Value,
			Expected:  record.Command.Expected,
			Applied:   record.Result.Applied,
			Result:    record.Result.Value,
		}
	}

	return index, data, dedup
}
