package raft

// validReadHeartbeatPayload validates correlated heartbeat
// messages before they can influence the local Raft term.
func validReadHeartbeatPayload(msg Message) bool {
	if msg.RequestID == 0 || msg.Term == 0 {
		return false
	}

	if msg.Index != 0 ||
		msg.LogTerm != 0 ||
		msg.Commit != 0 ||
		len(msg.Entries) != 0 {
		return false
	}

	switch msg.Type {
	case MsgHeartbeat:
		return !msg.Reject
	case MsgHeartbeatResp:
		return true
	default:
		return false
	}
}
