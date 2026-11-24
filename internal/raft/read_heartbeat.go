package raft

// sendReadHeartbeats sends correlated read confirmation
// probes to configured peers that have not acknowledged.
func (r *Raft) sendReadHeartbeats(request *pendingRead) {
	if request == nil {
		return
	}

	if r.volatile.Role != Leader {
		return
	}

	if request.term != r.hard.Term ||
		request.id == 0 {
		return
	}

	active, exists := r.pendingReads[request.id]

	if !exists || active != request {
		return
	}

	request.started = true

	if r.hasReadQuorum(request) {
		return
	}

	queued := make(map[NodeID]struct{})

	for _, msg := range r.messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID == request.id &&
			msg.Term == request.term {
			queued[msg.To] = struct{}{}
		}
	}

	for _, peer := range r.peers {
		if peer == 0 || peer == r.id {
			continue
		}

		if _, acknowledged := request.acks[peer]; acknowledged {
			continue
		}

		if _, exists := queued[peer]; exists {
			continue
		}

		r.messages = append(r.messages, Message{
			Type:      MsgHeartbeat,
			From:      r.id,
			To:        peer,
			Term:      request.term,
			RequestID: request.id,
		})

		queued[peer] = struct{}{}
	}
}
