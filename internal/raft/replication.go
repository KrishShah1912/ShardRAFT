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

	if r.pendingAppend[peer] == nil {
		r.pendingAppend[peer] = make(map[uint64]uint64)
	}

	r.pendingAppend[peer][requestID] = lastIndex

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
	reply := Message{
		Type:      MsgAppendResp,
		From:      r.id,
		To:        msg.From,
		Term:      r.hard.Term,
		Index:     msg.Index,
		Reject:    true,
		RequestID: msg.RequestID,
	}

	if msg.Term < r.hard.Term {
		r.messages = append(r.messages, reply)
		return
	}

	r.becomeFollower(msg.Term, msg.From)
	reply.Term = r.hard.Term

	// The follower cannot verify an entry older than
	// its snapshot boundary.
	if msg.Index < r.snapshotIndex {
		r.messages = append(r.messages, reply)
		return
	}

	if msg.Index > r.lastIndex() {
		r.messages = append(r.messages, reply)
		return
	}

	if r.termAt(msg.Index) != msg.LogTerm {
		r.messages = append(r.messages, reply)
		return
	}

	// Validate all incoming indexes before mutation.
	for i, entry := range msg.Entries {
		expected := msg.Index + 1 + uint64(i)
		if entry.Index != expected {
			r.messages = append(r.messages, reply)
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
			r.messages = append(r.messages, reply)
			return
		}

		if existing.Term != entry.Term {
			if entry.Index <= r.hard.Commit {
				r.messages = append(r.messages, reply)
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
				r.messages = append(r.messages, reply)
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
			r.hard.Commit = newCommit
			r.volatile.CommitIndex = newCommit
		}
	}

	reply.Reject = false
	reply.Index = msg.Index + uint64(len(msg.Entries))
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

	r.matchIndex[msg.From] = msg.Index
	r.nextIndex[msg.From] = msg.Index + 1
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
			r.hard.Commit = index
			r.volatile.CommitIndex = index
			return
		}
	}
}
