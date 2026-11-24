package raft

// isReadHeartbeatPeer checks whether the sender is
// a configured remote voter.
func (r *Raft) isReadHeartbeatPeer(id NodeID) bool {
	if id == 0 || id == r.id {
		return false
	}

	for _, peer := range r.peers {
		if peer == id {
			return true
		}
	}

	return false
}

// handleReadHeartbeat confirms leadership without
// modifying the follower's log or commit index.
func (r *Raft) handleReadHeartbeat(msg Message) {
	if msg.Type != MsgHeartbeat ||
		msg.RequestID == 0 ||
		msg.To != r.id ||
		!r.isReadHeartbeatPeer(msg.From) {
		return
	}

	reply := Message{
		Type:      MsgHeartbeatResp,
		From:      r.id,
		To:        msg.From,
		Term:      r.hard.Term,
		RequestID: msg.RequestID,
		Reject:    true,
	}

	if msg.Term != r.hard.Term ||
		msg.Term == 0 ||
		msg.Reject ||
		msg.Index != 0 ||
		msg.LogTerm != 0 ||
		msg.Commit != 0 ||
		len(msg.Entries) != 0 {
		r.messages = append(r.messages, reply)
		return
	}

	r.becomeFollower(msg.Term, msg.From)

	reply.Term = r.hard.Term
	reply.Reject = false

	r.messages = append(r.messages, reply)
}
