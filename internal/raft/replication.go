package raft

// appendEntry adds a new entry to the leader's log.
func (r *Raft) appendEntry(data []byte, entryType EntryType) Entry {
	var lastIndex uint64

	if len(r.log) > 0 {
		lastIndex = r.log[len(r.log)-1].Index
	}

	entry := Entry{
		Index: lastIndex + 1,
		Term:  r.hard.Term,
		Type:  entryType,
		Data:  append([]byte(nil), data...),
	}

	r.log = append(r.log, entry)

	return entry
}

func (r *Raft) termAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}

	if index > uint64(len(r.log)) {
		return 0
	}

	return r.log[index-1].Term
}

// sendAppend sends log entries to one follower.

func (r *Raft) sendAppend(peer NodeID) {
	next := uint64(1)

	if r.nextIndex != nil {
		if n, ok := r.nextIndex[peer]; ok {
			next = n
		}
	}

	lastIndex := uint64(len(r.log))

	if next < 1 || next > lastIndex+1 {
		return
	}

	prevIndex := next - 1
	entries := make([]Entry, 0, lastIndex-next+1)

	for _, entry := range r.log[next-1:] {
		copied := entry
		copied.Data = append([]byte(nil), entry.Data...)
		entries = append(entries, copied)
	}

	// NEW: Generate a unique request ID.
	r.nextRequestID++
	requestID := r.nextRequestID

	// NEW: Initialize request tracking for this follower.
	if r.pendingAppend[peer] == nil {
		r.pendingAppend[peer] = make(map[uint64]uint64)
	}

	// NEW: Record the highest log index covered by this request.
	r.pendingAppend[peer][requestID] = lastIndex

	r.messages = append(r.messages, Message{
		Type:      MsgAppend,
		From:      r.id,
		To:        peer,
		Term:      r.hard.Term,
		Index:     prevIndex,
		LogTerm:   r.termAt(prevIndex),
		Commit:    r.hard.Commit,
		RequestID: requestID, // NEW: Include request ID.
		Entries:   entries,
	})
}

// replicate sends pending entries to every follower.
func (r *Raft) replicate() {
	for _, peer := range r.peers {
		r.sendAppend(peer)
	}
}

func (r *Raft) handleAppendEntries(msg Message) {

	reply := Message{
		Type:      MsgAppendResp,
		From:      r.id,
		To:        msg.From,
		Term:      r.hard.Term,
		Index:     msg.Index,
		Reject:    true,
		RequestID: msg.RequestID,
	}

	// Reject messages from older terms.
	if msg.Term < r.hard.Term {
		r.messages = append(r.messages, reply)
		return
	}

	r.becomeFollower(msg.Term, msg.From)
	reply.Term = r.hard.Term

	// Check whether the previous log entry exists.
	if msg.Index > uint64(len(r.log)) {
		r.messages = append(r.messages, reply)
		return
	}

	// Check whether the previous entry has the right term.
	if r.termAt(msg.Index) != msg.LogTerm {
		r.messages = append(r.messages, reply)
		return
	}

	// Validate ALL incoming indexes before changing the log.
	for i, entry := range msg.Entries {
		expectedIndex := msg.Index + 1 + uint64(i)

		if entry.Index != expectedIndex {
			r.messages = append(r.messages, reply)
			return
		}
	}

	// Find the first actual term conflict or missing entry.
	appendFrom := len(msg.Entries)

	for i, entry := range msg.Entries {
		index := entry.Index

		if index > uint64(len(r.log)) {
			appendFrom = i
			break
		}

		existing := r.log[index-1]

		if existing.Term != entry.Term {
			// Never overwrite committed entries.
			if index <= r.hard.Commit {
				r.messages = append(r.messages, reply)
				return
			}

			appendFrom = i
			break
		}
	}

	// All validation has passed. Now modify the log.
	if appendFrom < len(msg.Entries) {
		firstNewIndex := msg.Entries[appendFrom].Index

		// Truncate only when an existing entry conflicts.
		if firstNewIndex <= uint64(len(r.log)) {
			r.log = r.log[:firstNewIndex-1]
		}

		for _, incoming := range msg.Entries[appendFrom:] {
			copied := incoming
			copied.Data = append([]byte(nil), incoming.Data...)
			r.log = append(r.log, copied)
		}
	}

	// Advance the follower's commit index safely.

	if msg.Commit > r.hard.Commit {
		// Determine the highest index covered by this AppendEntries.
		lastNewIndex := msg.Index + uint64(len(msg.Entries))

		// Never commit beyond the local log.
		if lastNewIndex > uint64(len(r.log)) {
			lastNewIndex = uint64(len(r.log))
		}

		// Never commit beyond the leader's commit index.
		newCommit := msg.Commit

		if newCommit > lastNewIndex {
			newCommit = lastNewIndex
		}

		// Raft safety: the commit index must only increase.
		if newCommit > r.hard.Commit {
			r.hard.Commit = newCommit
			r.volatile.CommitIndex = newCommit
		}
	}

	reply.Reject = false
	reply.Index = msg.Index + uint64(len(msg.Entries))
	r.messages = append(r.messages, reply)
}

func (r *Raft) handleAppendResponse(msg Message) {
	// Only the current leader handles responses.
	if r.volatile.Role != Leader {
		return
	}

	if msg.Term != r.hard.Term {
		return
	}

	// Accept responses only from configured followers.
	knownPeer := false
	for _, peer := range r.peers {
		if peer == msg.From {
			knownPeer = true
			break
		}
	}

	if !knownPeer {
		return
	}

	// Verify that this response belongs to a request
	// sent to this follower during the current leadership.
	pending := r.pendingAppend[msg.From]

	expectedIndex, exists := pending[msg.RequestID]
	if msg.RequestID == 0 || !exists {
		return
	}

	// Consume this request ID so duplicate responses
	// cannot be processed again.
	delete(pending, msg.RequestID)

	// A successful response must acknowledge exactly
	// the log position covered by the request.
	if !msg.Reject && msg.Index != expectedIndex {
		return
	}

	// Rejected responses will be handled separately.

	if msg.Reject {
		if msg.Index < r.matchIndex[msg.From] {
			return
		}

		next := r.nextIndex[msg.From]
		minNext := r.matchIndex[msg.From] + 1

		if next > minNext {
			r.nextIndex[msg.From] = next - 1
		}

		if r.nextIndex[msg.From] <= uint64(len(r.log)) {
			r.sendAppend(msg.From)
		}
		return
	}

	// A follower cannot acknowledge entries beyond our log.
	lastIndex := uint64(len(r.log))
	if msg.Index > lastIndex {
		return
	}

	// Never move confirmed progress backwards.
	if msg.Index <= r.matchIndex[msg.From] {
		return
	}

	r.matchIndex[msg.From] = msg.Index
	r.nextIndex[msg.From] = msg.Index + 1

	// Check whether a majority has replicated an entry.
	r.maybeCommit()
}

func (r *Raft) maybeCommit() {
	if r.volatile.Role != Leader {
		return
	}

	// Check possible commit indexes from highest to lowest.
	for index := uint64(len(r.log)); index > r.hard.Commit; index-- {

		// Raft only directly commits entries from
		// the leader's current term.
		if r.termAt(index) != r.hard.Term {
			continue
		}

		// The leader has its own log entry.
		count := 1

		for _, peer := range r.peers {
			if r.matchIndex[peer] >= index {
				count++
			}
		}

		if count >= r.majority() {
			r.hard.Commit = index
			r.volatile.CommitIndex = index
			return
		}
	}
}
