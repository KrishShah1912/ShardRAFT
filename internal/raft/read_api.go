package raft

// beginReadIndex admits a new read confirmation request.
//
// A successful return means that the request was accepted,
// not that the state machine is ready to serve the read.
func (r *Raft) beginReadIndex(ctx []byte) error {
	if r.volatile.Role != Leader {
		return ErrNotLeader
	}

	if len(ctx) == 0 || len(ctx) > maxReadContextSize {
		return ErrInvalidReadContext
	}

	index, err := r.readCommitBarrier()
	if err != nil {
		return err
	}

	// Bound the entire outstanding read lifecycle,
	// including confirmations awaiting consumption.
	if len(r.pendingReads)+len(r.completedReads) >= maxPendingReads {
		return ErrTooManyPendingReads
	}

	request, err := r.registerPendingRead(ctx, index)
	if err != nil {
		return err
	}

	request.autoComplete = true

	r.sendReadHeartbeats(request)

	// A single-node cluster already has a majority
	// after starting the confirmation round.
	r.completePendingRead(request)

	return nil
}
