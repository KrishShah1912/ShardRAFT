package raft

// invalidateReads discards volatile read requests and
// confirmations associated with an obsolete leadership.
//
// Request IDs remain monotonic throughout the lifetime
// of the Raft instance.
func (r *Raft) invalidateReads() {
	r.clearPendingReads()
	r.clearCompletedReads()

	// Remove queued outgoing read probes while preserving
	// ordinary heartbeats and replication messages.
	kept := r.messages[:0]

	for _, msg := range r.messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID != 0 {
			continue
		}

		kept = append(kept, msg)
	}

	clear(r.messages[len(kept):])
	r.messages = kept
}
