package raft

// ReadState represents a quorum-confirmed read barrier.
//
// The caller must wait until the state machine has applied
// at least Index before serving the corresponding read.
type ReadState struct {
	RequestID uint64
	Context   []byte
	Term      uint64
	Index     uint64
}

// completePendingRead confirms an active read request
// without modifying the replicated log or hard state.
func (r *Raft) completePendingRead(request *pendingRead) bool {
	if request == nil {
		return false
	}

	if !r.hasReadQuorum(request) {
		return false
	}

	if !r.hasCommittedCurrentTerm() {
		return false
	}

	if request.index == 0 ||
		request.index > r.hard.Commit {
		return false
	}

	// Bound completed results until the caller consumes them.
	if len(r.completedReads) >= maxPendingReads {
		return false
	}

	state := ReadState{
		RequestID: request.id,
		Context:   append([]byte(nil), request.context...),
		Term:      request.term,
		Index:     request.index,
	}

	r.completedReads = append(r.completedReads, state)
	r.removePendingRead(request.id)

	return true
}

// cloneReadStates returns an independent copy of completed
// read confirmations, including their context bytes.
func cloneReadStates(states []ReadState) []ReadState {
	if len(states) == 0 {
		return nil
	}

	copied := make([]ReadState, len(states))

	for i, state := range states {
		copied[i] = state
		copied[i].Context = append(
			[]byte(nil),
			state.Context...,
		)
	}

	return copied
}

// completedReadCount reports queued read confirmations.
func (r *Raft) completedReadCount() int {
	return len(r.completedReads)
}

// clearCompletedReads discards queued confirmations.
// Leadership invalidation and Ready integration will
// use this helper in subsequent implementation steps.
func (r *Raft) clearCompletedReads() {
	r.completedReads = nil
}
