package raft

import "bytes"

// CancelReadIndex removes outstanding read requests and
// completed confirmations matching the supplied leadership
// term and opaque context.
//
// All matching requests are canceled. Callers requiring
// independent cancellation must use unique contexts.
//
// Queued outgoing probes belonging to canceled requests
// are removed without affecting unrelated Raft messages.
//
// Cancellation does not modify persistent Raft state,
// replication progress, or the request ID sequence.
//
// The caller must serialize this operation with all
// other mutations of the Raft node.
//
// Repeated cancellation is safe.
func (r *Raft) CancelReadIndex(
	term uint64,
	ctx []byte,
) bool {
	if term == 0 ||
		len(ctx) == 0 ||
		len(ctx) > maxReadContextSize {
		return false
	}

	canceled := make(map[uint64]struct{})

	for id, request := range r.pendingReads {
		if request == nil ||
			request.term != term ||
			!bytes.Equal(request.context, ctx) {
			continue
		}

		canceled[id] = struct{}{}
		r.removePendingRead(id)
	}

	kept := r.completedReads[:0]

	for _, state := range r.completedReads {
		if state.Term == term &&
			bytes.Equal(state.Context, ctx) {
			canceled[state.RequestID] = struct{}{}
			continue
		}

		kept = append(kept, state)
	}

	clear(r.completedReads[len(kept):])

	if len(kept) == 0 {
		r.clearCompletedReads()
	} else {
		r.completedReads = kept
	}

	if len(canceled) == 0 {
		return false
	}

	r.removeCanceledReadProbes(term, canceled)

	return true
}

// removeCanceledReadProbes removes only outgoing
// correlated heartbeat probes for canceled identities.
func (r *Raft) removeCanceledReadProbes(
	term uint64,
	canceled map[uint64]struct{},
) {
	kept := r.messages[:0]

	for _, msg := range r.messages {
		if msg.Type == MsgHeartbeat &&
			msg.From == r.id &&
			msg.Term == term &&
			msg.RequestID != 0 {
			if _, exists := canceled[msg.RequestID]; exists {
				continue
			}
		}

		kept = append(kept, msg)
	}

	clear(r.messages[len(kept):])
	r.messages = kept
}
