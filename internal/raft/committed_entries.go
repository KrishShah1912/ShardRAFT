package raft

// CommittedEntry returns a copy of a committed log entry.
//
// Entries compacted into snapshots are unavailable.
// The snapshot boundary can be inspected using
// SnapshotMetadata and TermAt.
func (r *Raft) CommittedEntry(index uint64) (Entry, bool) {
	if index == 0 || index > r.hard.Commit {
		return Entry{}, false
	}

	entry, ok := r.entryAt(index)
	if !ok {
		return Entry{}, false
	}

	entry.Data = append([]byte(nil), entry.Data...)
	return entry, true
}
