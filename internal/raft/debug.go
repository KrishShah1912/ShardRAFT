package raft

type PeerReplicationDebug struct {
	NodeID        NodeID            `json:"node_id"`
	NextIndex     uint64            `json:"next_index"`
	MatchIndex    uint64            `json:"match_index"`
	PendingAppend map[uint64]uint64 `json:"pending_append"`
}

type DebugState struct {
	ID               NodeID                 `json:"id"`
	Role             Role                   `json:"role"`
	Term             uint64                 `json:"term"`
	LeaderID         NodeID                 `json:"leader_id"`
	CommitIndex      uint64                 `json:"commit_index"`
	AppliedIndex     uint64                 `json:"applied_index"`
	FirstLogIndex    uint64                 `json:"first_log_index"`
	LastLogIndex     uint64                 `json:"last_log_index"`
	SnapshotIndex    uint64                 `json:"snapshot_index"`
	SnapshotTerm     uint64                 `json:"snapshot_term"`
	ElectionElapsed  int                    `json:"election_elapsed"`
	ElectionTimeout  int                    `json:"election_timeout"`
	HeartbeatElapsed int                    `json:"heartbeat_elapsed"`
	HeartbeatTimeout int                    `json:"heartbeat_timeout"`
	Peers            []PeerReplicationDebug `json:"peers"`
}

type DebugLogEntry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data,omitempty"`
}

type DebugLog struct {
	SnapshotIndex uint64          `json:"snapshot_index"`
	SnapshotTerm  uint64          `json:"snapshot_term"`
	FirstIndex    uint64          `json:"first_index"`
	LastIndex     uint64          `json:"last_index"`
	CommitIndex   uint64          `json:"commit_index"`
	AppliedIndex  uint64          `json:"applied_index"`
	Entries       []DebugLogEntry `json:"entries"`
}

func (r *Raft) DebugState() DebugState {
	peers := make([]PeerReplicationDebug, 0, len(r.peers))

	for _, peer := range r.peers {
		pending := make(map[uint64]uint64)

		for requestID, index := range r.pendingAppend[peer] {
			pending[requestID] = index
		}

		peers = append(peers, PeerReplicationDebug{
			NodeID:        peer,
			NextIndex:     r.nextIndex[peer],
			MatchIndex:    r.matchIndex[peer],
			PendingAppend: pending,
		})
	}

	return DebugState{
		ID:               r.id,
		Role:             r.volatile.Role,
		Term:             r.hard.Term,
		LeaderID:         r.volatile.LeaderID,
		CommitIndex:      r.hard.Commit,
		AppliedIndex:     r.volatile.AppliedIndex,
		FirstLogIndex:    r.firstIndex(),
		LastLogIndex:     r.lastIndex(),
		SnapshotIndex:    r.snapshotIndex,
		SnapshotTerm:     r.snapshotTerm,
		ElectionElapsed:  r.volatile.ElectionElapsed,
		ElectionTimeout:  r.volatile.ElectionTimeout,
		HeartbeatElapsed: r.volatile.HeartbeatElapsed,
		HeartbeatTimeout: r.heartbeatTimeout,
		Peers:            peers,
	}
}

func (r *Raft) DebugLog() DebugLog {
	entries := make([]DebugLogEntry, 0, len(r.log))

	for _, entry := range r.log {
		entries = append(entries, DebugLogEntry{
			Index: entry.Index,
			Term:  entry.Term,
			Type:  entry.Type,
			Data:  append([]byte(nil), entry.Data...),
		})
	}

	return DebugLog{
		SnapshotIndex: r.snapshotIndex,
		SnapshotTerm:  r.snapshotTerm,
		FirstIndex:    r.firstIndex(),
		LastIndex:     r.lastIndex(),
		CommitIndex:   r.hard.Commit,
		AppliedIndex:  r.volatile.AppliedIndex,
		Entries:       entries,
	}
}
