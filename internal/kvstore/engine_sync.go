package kvstore

import (
	"errors"
	"fmt"
)

// SyncEngine rebuilds Pebble from the current applied KV state.
func (s *Store) SyncEngine() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.engine == nil {
		return errors.New("no KV engine attached")
	}

	if err := s.engine.Clear(); err != nil {
		return fmt.Errorf("clear KV engine: %w", err)
	}

	for key, value := range s.data {
		if err := s.engine.Put(key, value); err != nil {
			return fmt.Errorf("sync KV key %q: %w", key, err)
		}
	}

	return nil
}
