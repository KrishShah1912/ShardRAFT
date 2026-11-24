package raft

// handleReadHeartbeatResp records a valid acknowledgment
// for an active read request.
//
// Acknowledgments are independent of log replication.
// Recording an acknowledgment does not complete a read.
func (r *Raft) handleReadHeartbeatResp(msg Message) {
	if msg.Type != MsgHeartbeatResp ||
		msg.RequestID == 0 ||
		msg.To != r.id ||
		!r.isReadHeartbeatPeer(msg.From) {
		return
	}

	if r.volatile.Role != Leader ||
		msg.Term == 0 ||
		msg.Term != r.hard.Term ||
		msg.Reject {
		return
	}

	if msg.Index != 0 ||
		msg.LogTerm != 0 ||
		msg.Commit != 0 ||
		len(msg.Entries) != 0 {
		return
	}

	request, exists := r.pendingReads[msg.RequestID]

	if !exists || request == nil {
		return
	}

	if request.id != msg.RequestID ||
		request.term != r.hard.Term ||
		!request.started {
		return
	}

	if _, acknowledged := request.acks[msg.From]; acknowledged {
		return
	}

	request.acks[msg.From] = struct{}{}

	if request.autoComplete {
		r.completePendingRead(request)
	}
}
