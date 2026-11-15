package shard

import (
	"github.com/KrishShah1912/shardraft/internal/kvstore"
)

// CheckedRequestResult returns the result of a committed
// request only when its original command matches.
//
// found=false means the request has not yet been applied.
// An identical retry returns the original result.
// A conflicting retry returns kvstore.ErrRequestConflict.
func (s *Shard) CheckedRequestResult(
	command kvstore.Command,
) (kvstore.RequestResult, bool, error) {
	if err := command.Validate(); err != nil {
		return kvstore.RequestResult{}, false, err
	}

	if command.ClientID == "" {
		return kvstore.RequestResult{}, false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return kvstore.RequestResult{}, false, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return kvstore.RequestResult{}, false, ErrStopped
	}

	return s.store.CheckRequest(command)
}
