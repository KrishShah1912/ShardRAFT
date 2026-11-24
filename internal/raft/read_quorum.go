package raft

// hasReadQuorum reports whether an active read request
// has acknowledgments from a majority of configured
// distinct voters in the current leadership term.
//
// Quorum confirmation alone does not complete a read.
// Current-term commit and application barriers are
// enforced separately.
func (r *Raft) hasReadQuorum(request *pendingRead) bool {
	if request == nil ||
		request.id == 0 ||
		request.term == 0 ||
		r.volatile.Role != Leader ||
		request.term != r.hard.Term {
		return false
	}

	active, exists := r.pendingReads[request.id]

	if !exists || active != request {
		return false
	}

	if !request.started {
		return false
	}

	voters := map[NodeID]struct{}{
		r.id: {},
	}

	for _, peer := range r.peers {
		if peer != 0 && peer != r.id {
			voters[peer] = struct{}{}
		}
	}

	acknowledged := 0

	for id := range request.acks {
		if _, configured := voters[id]; configured {
			acknowledged++
		}
	}

	required := len(voters)/2 + 1

	return acknowledged >= required
}
