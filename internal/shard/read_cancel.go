package shard

// cancelLinearizableRead releases both shard-level and
// Raft-level tracking for an owned linearizable read.
//
// The caller must hold s.mu.
//
// Only the exact registered request may cancel its
// corresponding Raft read. Unknown, copied, and obsolete
// request objects must not affect active requests.
//
// Cancellation is idempotent and does not perform
// persistent writes or invoke the Raft driver.
func (s *Shard) cancelLinearizableRead(
	request *pendingShardRead,
) bool {
	if !s.removePendingRead(request) {
		return false
	}

	if s.node != nil {
		s.node.CancelReadIndex(
			request.term,
			request.context,
		)
	}

	return true
}
