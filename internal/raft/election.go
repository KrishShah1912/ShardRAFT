package raft

func (r *Raft) becomeFollower(term uint64, leader NodeID) {
	if term > r.hard.Term {
		r.hard.Term = term
		r.hard.Vote = 0
	}

	r.volatile.Role = Follower
	r.volatile.LeaderID = leader
	r.volatile.ElectionElapsed = 0
	r.votes = make(map[NodeID]bool)
}

func (r *Raft) startElection() {
	r.volatile.Role = Candidate
	r.volatile.LeaderID = 0
	r.volatile.ElectionElapsed = 0

	r.hard.Term++
	r.hard.Vote = r.id

	r.votes = map[NodeID]bool{
		r.id: true,
	}

	// A single-node cluster already has a majority.
	if len(r.votes) >= r.majority() {
		r.becomeLeader()
		return
	}

	lastIndex, lastTerm := r.lastLogInfo()

	for _, peer := range r.peers {
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
	r.volatile.Role = Leader
	r.volatile.LeaderID = r.id
	r.volatile.HeartbeatElapsed = 0

	// Initialize follower replication positions.
	lastIndex := uint64(len(r.log))

	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIndex + 1
		r.matchIndex[peer] = 0
	}

	// Append a no-op entry in the new leader's term.
	r.appendEntry(nil, EntryNoop)

	// Replicate the no-op to followers.
	r.replicate()

	// A single-node cluster can commit immediately.
	r.maybeCommit()
}

func (r *Raft) lastLogInfo() (uint64, uint64) {
	if len(r.log) == 0 {
		return 0, 0
	}

	last := r.log[len(r.log)-1]
	return last.Index, last.Term
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

	// Reject heartbeats from older terms.
	if msg.Term < r.hard.Term {
		return
	}

	r.becomeFollower(msg.Term, msg.From)

	// Verify that the follower has the same log
	// entry at the index confirmed by the leader.
	if msg.Index > uint64(len(r.log)) ||
		r.termAt(msg.Index) != msg.LogTerm {
		return
	}

	// A heartbeat can only commit entries through
	// the verified matching log position.
	newCommit := msg.Commit

	if newCommit > msg.Index {
		newCommit = msg.Index
	}

	if newCommit > r.hard.Commit {
		r.hard.Commit = newCommit
		r.volatile.CommitIndex = newCommit
	}
}
