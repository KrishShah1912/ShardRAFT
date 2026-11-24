package raft

func (r *Raft) becomeFollower(term uint64, leader NodeID) {

	if r.volatile.Role == Leader ||
		term != r.hard.Term {
		r.invalidateReads()
	}

	previousRole := r.volatile.Role
	previousTerm := r.hard.Term

	if term > r.hard.Term {
		r.hard.Term = term
		r.hard.Vote = 0
	}

	r.volatile.Role = Follower
	r.volatile.LeaderID = leader
	r.volatile.ElectionElapsed = 0
	r.resetElectionTimeout()
	r.votes = make(map[NodeID]bool)

	if previousTerm != r.hard.Term {
		r.emit(EventTermChanged, map[string]any{
			"previous_term": previousTerm,
			"term":          r.hard.Term,
		})
	}

	if previousRole != Follower {
		r.emit(EventRoleChanged, map[string]any{
			"previous_role": previousRole,
			"role":          Follower,
			"leader_id":     leader,
		})
	}
}

func (r *Raft) startElection() {
	r.invalidateReads()

	previousRole := r.volatile.Role
	previousTerm := r.hard.Term

	r.volatile.Role = Candidate
	r.volatile.LeaderID = 0
	r.volatile.ElectionElapsed = 0
	r.resetElectionTimeout()

	r.hard.Term++
	r.hard.Vote = r.id

	r.emit(EventTermChanged, map[string]any{
		"previous_term": previousTerm,
		"term":          r.hard.Term,
	})

	r.emit(EventRoleChanged, map[string]any{
		"previous_role": previousRole,
		"role":          Candidate,
	})

	r.emit(EventElectionStarted, map[string]any{
		"term": r.hard.Term,
	})

	r.votes = map[NodeID]bool{
		r.id: true,
	}

	if len(r.votes) >= r.majority() {
		r.becomeLeader()
		return
	}

	lastIndex, lastTerm := r.lastLogInfo()

	for _, peer := range r.peers {
		r.emit(EventVoteRequested, map[string]any{
			"to":         peer,
			"term":       r.hard.Term,
			"last_index": lastIndex,
			"last_term":  lastTerm,
		})

		r.messages = append(r.messages, Message{
			Type:    MsgVote,
			From:    r.id,
			To:      peer,
			Term:    r.hard.Term,
			Index:   lastIndex,
			LogTerm: lastTerm,
		})
	}
}

func (r *Raft) becomeLeader() {
	r.invalidateReads()

	previousRole := r.volatile.Role

	r.volatile.Role = Leader
	r.volatile.LeaderID = r.id
	r.volatile.HeartbeatElapsed = 0

	lastIndex := r.lastIndex()

	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIndex + 1
		r.matchIndex[peer] = 0
	}

	r.emit(EventRoleChanged, map[string]any{
		"previous_role": previousRole,
		"role":          Leader,
		"leader_id":     r.id,
	})

	r.emit(EventLeaderElected, map[string]any{
		"leader_id": r.id,
		"term":      r.hard.Term,
	})

	entry := r.appendEntry(nil, EntryNoop)

	r.emit(EventNoopAppended, map[string]any{
		"index": entry.Index,
		"term":  entry.Term,
	})

	r.replicate()
	r.maybeCommit()
}

func (r *Raft) lastLogInfo() (uint64, uint64) {
	lastIndex := r.lastIndex()
	return lastIndex, r.termAt(lastIndex)
}

func (r *Raft) handleVoteRequest(msg Message) {
	granted := false

	if msg.Term == r.hard.Term {
		canVote := r.hard.Vote == 0 || r.hard.Vote == msg.From

		lastIndex, lastTerm := r.lastLogInfo()

		upToDate := msg.LogTerm > lastTerm ||
			(msg.LogTerm == lastTerm && msg.Index >= lastIndex)

		if canVote && upToDate {
			r.hard.Vote = msg.From
			r.volatile.ElectionElapsed = 0
			granted = true
		}
	}

	r.messages = append(r.messages, Message{
		Type:   MsgVoteResp,
		From:   r.id,
		To:     msg.From,
		Term:   r.hard.Term,
		Reject: !granted,
	})

	eventType := EventVoteRejected
	if granted {
		eventType = EventVoteGranted
	}

	r.emit(eventType, map[string]any{
		"candidate_id": msg.From,
		"term":         r.hard.Term,
	})
}

func (r *Raft) handleVoteResponse(msg Message) {
	if r.volatile.Role != Candidate ||
		msg.Term != r.hard.Term {
		return
	}

	if !msg.Reject {
		r.votes[msg.From] = true
	}

	if len(r.votes) >= r.majority() {
		r.becomeLeader()
	}
}

func (r *Raft) handleAppend(msg Message) {
	if msg.Type == MsgAppend {
		r.handleAppendEntries(msg)
		return
	}

	if msg.Term < r.hard.Term {
		return
	}

	r.becomeFollower(msg.Term, msg.From)

	// A compacted follower cannot verify entries
	// preceding its snapshot boundary.
	if msg.Index < r.snapshotIndex {
		return
	}

	if msg.Index > r.lastIndex() ||
		r.termAt(msg.Index) != msg.LogTerm {
		return
	}

	newCommit := msg.Commit
	if newCommit > msg.Index {
		newCommit = msg.Index
	}

	if newCommit > r.hard.Commit {
		previousCommit := r.hard.Commit

		r.hard.Commit = newCommit
		r.volatile.CommitIndex = newCommit

		r.emit(EventCommitAdvanced, map[string]any{
			"previous_commit": previousCommit,
			"commit_index":    newCommit,
			"source":          "heartbeat",
		})
	}
}
