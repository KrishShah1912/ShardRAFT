package raft

// appendEntry adds a new entry to the leader's log.
func (r *Raft) appendEntry(data []byte, entryType EntryType) Entry {
	entry := Entry{
		Index: r.lastIndex() + 1,
		Term:  r.hard.Term,
		Type:  entryType,
		Data:  append([]byte(nil), data...),
	}

	r.log = append(r.log, entry)

	r.emit(EventEntryAppended, map[string]any{
		"index": entry.Index,
		"term":  entry.Term,
		"type":  entry.Type,
	})

	return entry
}

// termAt returns the term at an absolute Raft index.
// The snapshot boundary has a known term even though
// its entry is no longer present in memory.
func (r *Raft) termAt(index uint64) uint64 {
	if index == r.snapshotIndex {
		return r.snapshotTerm
	}

	entry, ok := r.entryAt(index)
	if !ok {
		return 0
	}

	return entry.Term
}

// sendAppend sends log entries to one follower.
func (r *Raft) sendAppend(peer NodeID) {
	next := uint64(1)

	if n, ok := r.nextIndex[peer]; ok {
		next = n
	}

	lastIndex := r.lastIndex()

	// A follower behind the snapshot boundary will
	// eventually require snapshot installation.
	// That protocol is not implemented yet.
	if next <= r.snapshotIndex {
		return
	}

	if next > lastIndex+1 {
		return
	}

	prevIndex := next - 1

	entries := make([]Entry, 0)
	for _, entry := range r.log {
		if entry.Index >= next {
			copied := entry
			copied.Data = append([]byte(nil), entry.Data...)
			entries = append(entries, copied)
		}
	}

	r.nextRequestID++
	requestID := r.nextRequestID

	// Keep only the latest outstanding request per follower.
	// Older responses will be ignored.
	r.pendingAppend[peer] = map[uint64]uint64{
		requestID: lastIndex,
	}

	sendEvent := EventAppendSent
	if len(entries) == 0 {
		sendEvent = EventHeartbeatSent
	}

	r.emit(sendEvent, map[string]any{
		"to":             peer,
		"request_id":     requestID,
		"previous_index": prevIndex,
		"previous_term":  r.termAt(prevIndex),
		"entries":        len(entries),
		"commit_index":   r.hard.Commit,
	})

	r.messages = append(r.messages, Message{
		Type:      MsgAppend,
		From:      r.id,
		To:        peer,
		Term:      r.hard.Term,
		Index:     prevIndex,
		LogTerm:   r.termAt(prevIndex),
		Commit:    r.hard.Commit,
		RequestID: requestID,
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
	receiveEvent := EventAppendReceived
	if len(msg.Entries) == 0 {
		receiveEvent = EventHeartbeatReceived
	}

	r.emit(receiveEvent, map[string]any{
		"from":           msg.From,
		"request_id":     msg.RequestID,
		"previous_index": msg.Index,
		"entries":        len(msg.Entries),
		"leader_commit":  msg.Commit,
	})

	reply := Message{
		Type:      MsgAppendResp,
		From:      r.id,
		To:        msg.From,
		Term:      r.hard.Term,
		Index:     msg.Index,
		Reject:    true,
		RequestID: msg.RequestID,
	}

	reject := func(reason string) {
		r.emit(EventAppendRejected, map[string]any{
			"from":       msg.From,
			"request_id": msg.RequestID,
			"index":      msg.Index,
			"reason":     reason,
		})

		r.messages = append(r.messages, reply)
	}

	if msg.Term < r.hard.Term {
		reject("stale_term")
		return
	}

	r.becomeFollower(msg.Term, msg.From)
	reply.Term = r.hard.Term

	// The follower cannot verify an entry older than
	// its snapshot boundary.
	if msg.Index < r.snapshotIndex {
		reject("before_snapshot_boundary")
		return
	}

	if msg.Index > r.lastIndex() {
		reject("missing_previous_entry")
		return
	}

	if r.termAt(msg.Index) != msg.LogTerm {
		reject("previous_term_mismatch")
		return
	}

	// Validate all incoming indexes before mutation.
	for i, entry := range msg.Entries {
		expected := msg.Index + 1 + uint64(i)
		if entry.Index != expected {
			reject("invalid_entry_index")
			return
		}
	}

	appendFrom := len(msg.Entries)

	for i, entry := range msg.Entries {
		if entry.Index > r.lastIndex() {
			appendFrom = i
			break
		}

		existing, ok := r.entryAt(entry.Index)
		if !ok {
			reject("entry_unavailable")
			return
		}

		if existing.Term != entry.Term {
			if entry.Index <= r.hard.Commit {
				reject("committed_entry_conflict")
				return
			}

			appendFrom = i
			break
		}
	}

	if appendFrom < len(msg.Entries) {
		firstNewIndex := msg.Entries[appendFrom].Index

		if firstNewIndex <= r.lastIndex() {
			offset, ok := r.logOffset(firstNewIndex)
			if !ok {
				reject("invalid_log_offset")
				return
			}

			r.log = r.log[:offset]
		}

		for _, incoming := range msg.Entries[appendFrom:] {
			copied := incoming
			copied.Data = append([]byte(nil), incoming.Data...)
			r.log = append(r.log, copied)
		}
	}

	if msg.Commit > r.hard.Commit {
		lastNewIndex := msg.Index + uint64(len(msg.Entries))

		if lastNewIndex > r.lastIndex() {
			lastNewIndex = r.lastIndex()
		}

		newCommit := msg.Commit
		if newCommit > lastNewIndex {
			newCommit = lastNewIndex
		}

		if newCommit > r.hard.Commit {
			previousCommit := r.hard.Commit

			r.hard.Commit = newCommit
			r.volatile.CommitIndex = newCommit

			r.emit(EventCommitAdvanced, map[string]any{
				"previous_commit": previousCommit,
				"commit_index":    newCommit,
				"source":          "append_entries",
			})
		}
	}

	reply.Reject = false
	reply.Index = msg.Index + uint64(len(msg.Entries))

	acceptedEvent := EventAppendAccepted
	if len(msg.Entries) == 0 {
		acceptedEvent = EventHeartbeatAccepted
	}

	r.emit(acceptedEvent, map[string]any{
		"from":       msg.From,
		"request_id": msg.RequestID,
		"index":      reply.Index,
	})

	r.messages = append(r.messages, reply)
}

func (r *Raft) handleAppendResponse(msg Message) {
	if r.volatile.Role != Leader || msg.Term != r.hard.Term {
		return
	}

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

	pending := r.pendingAppend[msg.From]
	expectedIndex, exists := pending[msg.RequestID]

	if msg.RequestID == 0 || !exists {
		return
	}

	delete(pending, msg.RequestID)

	if !msg.Reject && msg.Index != expectedIndex {
		return
	}

	if msg.Reject {
		if msg.Index < r.matchIndex[msg.From] {
			return
		}

		next := r.nextIndex[msg.From]
		minNext := r.matchIndex[msg.From] + 1

		if next > minNext {
			r.nextIndex[msg.From] = next - 1
		}

		r.emit(EventAppendRejected, map[string]any{
			"from":       msg.From,
			"request_id": msg.RequestID,
			"index":      msg.Index,
			"next_index": r.nextIndex[msg.From],
			"reason":     "follower_rejected",
		})

		if r.nextIndex[msg.From] <= r.lastIndex() {
			r.sendAppend(msg.From)
		}

		return
	}

	if msg.Index > r.lastIndex() {
		return
	}

	if msg.Index <= r.matchIndex[msg.From] {
		return
	}

	previousMatch := r.matchIndex[msg.From]
	previousNext := r.nextIndex[msg.From]

	r.matchIndex[msg.From] = msg.Index
	r.nextIndex[msg.From] = msg.Index + 1

	r.emit(EventReplicationProgress, map[string]any{
		"peer_id":        msg.From,
		"previous_match": previousMatch,
		"match_index":    msg.Index,
		"previous_next":  previousNext,
		"next_index":     msg.Index + 1,
		"request_id":     msg.RequestID,
	})

	r.maybeCommit()
}

func (r *Raft) maybeCommit() {
	if r.volatile.Role != Leader {
		return
	}

	for index := r.lastIndex(); index > r.hard.Commit; index-- {
		if r.termAt(index) != r.hard.Term {
			continue
		}

		count := 1
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= index {
				count++
			}
		}

		if count >= r.majority() {
			previousCommit := r.hard.Commit

			r.hard.Commit = index
			r.volatile.CommitIndex = index

			r.emit(EventCommitAdvanced, map[string]any{
				"previous_commit": previousCommit,
				"commit_index":    index,
				"replicas":        count,
				"majority":        r.majority(),
				"source":          "leader_quorum",
			})

			return
		}
	}
}
