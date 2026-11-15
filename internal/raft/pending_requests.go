package raft

// PendingEntries returns copies of entries that have
// not yet been committed.
//
// The caller must serialize access to the Raft node.
func (r *Raft) PendingEntries() []Entry {
	var pending []Entry

	for _, entry := range r.log {
		if entry.Index > r.hard.Commit {
			pending = append(pending, entry)
		}
	}

	return cloneEntries(pending)
}
