package raft

// AdvanceReadStates acknowledges completed read barriers
// that the caller has successfully processed.
//
// Only matching request IDs and terms are removed.
// Pending requests and unacknowledged confirmations
// remain available.
//
// The caller must serialize this operation with all
// other mutations of the Raft node.
func (r *Raft) AdvanceReadStates(states []ReadState) {
	if len(states) == 0 ||
		len(r.completedReads) == 0 {
		return
	}

	type identity struct {
		id   uint64
		term uint64
	}

	acknowledged := make(
		map[identity]struct{},
		len(states),
	)

	for _, state := range states {
		if state.RequestID == 0 ||
			state.Term == 0 {
			continue
		}

		acknowledged[identity{
			id:   state.RequestID,
			term: state.Term,
		}] = struct{}{}
	}

	kept := r.completedReads[:0]

	for _, state := range r.completedReads {
		key := identity{
			id:   state.RequestID,
			term: state.Term,
		}

		if _, exists := acknowledged[key]; exists {
			continue
		}

		kept = append(kept, state)
	}

	clear(r.completedReads[len(kept):])

	if len(kept) == 0 {
		r.clearCompletedReads()
		return
	}

	r.completedReads = kept
}
