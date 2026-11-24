package raft

import "sort"

// retryPendingReads retransmits outstanding public read
// confirmations during the periodic heartbeat cycle.
func (r *Raft) retryPendingReads() {
	if r.volatile.Role != Leader {
		return
	}

	if !r.hasCommittedCurrentTerm() {
		return
	}

	if len(r.pendingReads) == 0 {
		return
	}

	// Stable ordering preserves deterministic simulation.
	ids := make([]uint64, 0, len(r.pendingReads))

	for id, request := range r.pendingReads {
		if request == nil ||
			!request.autoComplete ||
			!request.started ||
			request.term != r.hard.Term {
			continue
		}

		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	for _, id := range ids {
		request := r.pendingReads[id]

		if request == nil {
			continue
		}

		if r.completePendingRead(request) {
			continue
		}

		r.sendReadHeartbeats(request)
	}
}
