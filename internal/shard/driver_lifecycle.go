package shard

// processDriver processes pending Raft work and handles
// terminal driver failures consistently.
//
// The caller must hold s.mu.
func (s *Shard) processDriver() error {
	err := s.driver.Process()

	if err != nil || s.driver.Stopped() {
		s.stopped = true
		s.failPendingReads(ErrStopped)

		if err == nil {
			return ErrStopped
		}

		return err
	}

	s.reconcilePendingReads()

	return nil
}
