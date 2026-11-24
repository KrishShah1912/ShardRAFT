package raft

import "errors"

const (
	maxPendingReads    = 1024
	maxReadContextSize = 4096
)

var (
	ErrInvalidReadContext = errors.New(
		"invalid read context",
	)

	ErrTooManyPendingReads = errors.New(
		"too many pending read requests",
	)

	ErrReadIDExhausted = errors.New(
		"read request ID exhausted",
	)
)

// pendingRead represents a volatile read confirmation
// request belonging to a specific leadership term.
//
// The caller must serialize access to Raft.
type pendingRead struct {
	// Public API requests complete automatically on quorum.
	autoComplete bool
	started      bool
	id           uint64
	context      []byte
	term         uint64
	index        uint64
	acks         map[NodeID]struct{}
}

// registerPendingRead creates an independent pending
// read request without modifying the Raft log.
func (r *Raft) registerPendingRead(
	ctx []byte,
	index uint64,
) (*pendingRead, error) {
	if r.volatile.Role != Leader {
		return nil, ErrNotLeader
	}

	if len(ctx) == 0 || len(ctx) > maxReadContextSize {
		return nil, ErrInvalidReadContext
	}

	if len(r.pendingReads) >= maxPendingReads {
		return nil, ErrTooManyPendingReads
	}

	if r.nextReadID == ^uint64(0) {
		return nil, ErrReadIDExhausted
	}

	r.nextReadID++

	request := &pendingRead{
		id:      r.nextReadID,
		context: append([]byte(nil), ctx...),
		term:    r.hard.Term,
		index:   index,
		acks: map[NodeID]struct{}{
			r.id: {},
		},
	}

	if r.pendingReads == nil {
		r.pendingReads = make(map[uint64]*pendingRead)
	}

	r.pendingReads[request.id] = request

	return request, nil
}

// removePendingRead removes a request from volatile
// tracking. Repeated removal is safe.
func (r *Raft) removePendingRead(id uint64) {
	delete(r.pendingReads, id)
}

// clearPendingReads invalidates all outstanding
// requests without resetting the request ID sequence.
func (r *Raft) clearPendingReads() {
	clear(r.pendingReads)
}

// pendingReadCount reports the number of tracked
// requests. Access must be serialized by the caller.
func (r *Raft) pendingReadCount() int {
	return len(r.pendingReads)
}
