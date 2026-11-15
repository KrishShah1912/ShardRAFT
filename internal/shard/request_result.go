package shard

import (
	"github.com/KrishShah1912/shardraft/internal/kvstore"
)

// RequestResult returns the outcome of a committed,
// applied client request.
//
// The caller can use this to determine whether a
// previously proposed operation has finished.
//
// A missing result does not prove the request failed.
// It may still be pending or may have been overwritten
// during a leader change.
func (s *Shard) RequestResult(
	clientID, requestID string,
) (kvstore.RequestResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstore.RequestResult{}, false, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return kvstore.RequestResult{}, false, ErrStopped
	}

	result, found := s.store.RequestResult(
		clientID,
		requestID,
	)

	return result, found, nil
}
