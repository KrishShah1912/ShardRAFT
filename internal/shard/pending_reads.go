package shard

import (
	"encoding/binary"
	"errors"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

const maxPendingShardReads = 1024

var (
	ErrTooManyPendingReads = errors.New(
		"too many pending shard reads",
	)

	ErrReadIDExhausted = errors.New(
		"shard read request ID exhausted",
	)

	ErrInvalidReadTerm = errors.New(
		"invalid shard read term",
	)
)

// shardReadResult contains an internal quorum confirmation
// or a terminal request error.
//
// A confirmation is not itself permission to return KV data.
type shardReadResult struct {
	state raft.ReadState
	err   error
}

// pendingShardRead tracks one outstanding linearizable read.
type pendingShardRead struct {
	confirmation *raft.ReadState
	id           uint64
	context      []byte
	term         uint64
	done         chan shardReadResult
}

// registerPendingRead creates a unique volatile read request.
//
// The caller must hold s.mu.
func (s *Shard) registerPendingRead(
	term uint64,
) (*pendingShardRead, error) {
	if term == 0 {
		return nil, ErrInvalidReadTerm
	}

	if len(s.pendingReads) >= maxPendingShardReads {
		return nil, ErrTooManyPendingReads
	}

	if s.nextReadID == ^uint64(0) {
		return nil, ErrReadIDExhausted
	}

	s.nextReadID++

	// The context is an opaque, fixed-size request identity.
	// It is independent of the requested KV key.
	ctx := make([]byte, 16)

	binary.BigEndian.PutUint64(ctx[:8], term)
	binary.BigEndian.PutUint64(ctx[8:], s.nextReadID)

	request := &pendingShardRead{
		id:      s.nextReadID,
		context: ctx,
		term:    term,
		done:    make(chan shardReadResult, 1),
	}

	if s.pendingReads == nil {
		s.pendingReads = make(
			map[string]*pendingShardRead,
		)
	}

	s.pendingReads[string(ctx)] = request

	return request, nil
}

// removePendingRead removes only the matching request.
//
// The caller must hold s.mu.
func (s *Shard) removePendingRead(
	request *pendingShardRead,
) bool {
	if request == nil {
		return false
	}

	key := string(request.context)

	if s.pendingReads[key] != request {
		return false
	}

	delete(s.pendingReads, key)

	if len(s.pendingReads) == 0 {
		s.pendingReads = nil
	}

	return true
}

// pendingReadCount returns the number of outstanding reads.
//
// The caller must hold s.mu.
func (s *Shard) pendingReadCount() int {
	return len(s.pendingReads)
}

// failPendingReads releases every outstanding request.
//
// Notifications never block while holding the shard mutex.
//
// The caller must hold s.mu.
func (s *Shard) failPendingReads(err error) {
	if err == nil {
		return
	}

	for _, request := range s.pendingReads {
		s.cancelLinearizableRead(request)

		// A terminal failure supersedes any queued
		// internal confirmation.
		select {
		case <-request.done:
		default:
		}

		select {
		case request.done <- shardReadResult{err: err}:
		default:
		}
	}

	clear(s.pendingReads)
	s.pendingReads = nil
}
