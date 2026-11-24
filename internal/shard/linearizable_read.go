package shard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

const defaultLinearizableReadTimeout = 3 * time.Second

var (
	ErrInvalidReadKey = errors.New(
		"linearizable read key must not be empty",
	)

	ErrInvalidReadContext = errors.New(
		"linearizable read context must not be nil",
	)

	ErrInvalidReadConfirmation = errors.New(
		"invalid linearizable read confirmation",
	)
)

// GetLinearizable returns a KV value only after a fresh
// Raft quorum confirmation and application barrier.
//
// Unlike Get, this method requires current leadership.
//
// The shard mutex is released while waiting so that Tick
// and Step can continue processing Raft messages.
func (s *Shard) GetLinearizable(
	ctx context.Context,
	key string,
) (string, bool, error) {
	if ctx == nil {
		return "", false, ErrInvalidReadContext
	}

	if key == "" {
		return "", false, ErrInvalidReadKey
	}

	// Bound the wait even when the caller supplies no deadline.
	ctx, cancel := context.WithTimeout(
		ctx,
		defaultLinearizableReadTimeout,
	)
	defer cancel()

	if err := ctx.Err(); err != nil {
		return "", false, err
	}

	request, err := s.beginLinearizableRead(ctx)
	if err != nil {
		return "", false, err
	}

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.cancelLinearizableRead(request)
	}()

	select {
	case <-ctx.Done():
		return "", false, ctx.Err()

	case result := <-request.done:
		if result.err != nil {
			return "", false, result.err
		}

		return s.finishLinearizableRead(
			ctx,
			key,
			request,
			result.state,
		)
	}
}

// beginLinearizableRead registers the request and submits
// it to Raft while holding the shard mutex.
func (s *Shard) beginLinearizableRead(
	ctx context.Context,
) (*pendingShardRead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s.closed {
		return nil, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return nil, ErrStopped
	}

	status := s.node.Status()

	if status.Role != raft.Leader {
		return nil, raft.ErrNotLeader
	}

	request, err := s.registerPendingRead(status.Term)
	if err != nil {
		return nil, err
	}

	if err := s.node.ReadIndex(request.context); err != nil {
		s.cancelLinearizableRead(request)
		return nil, err
	}

	// A single-node leader may confirm immediately.
	// Multi-node leaders send their quorum probes here.
	if err := s.processDriver(); err != nil {

		return nil, fmt.Errorf(
			"process linearizable read: %w",
			err,
		)
	}

	return request, nil
}

// finishLinearizableRead validates the internal
// confirmation and reads the KV state atomically with
// respect to other shard operations.
func (s *Shard) finishLinearizableRead(
	ctx context.Context,
	key string,
	request *pendingShardRead,
	state raft.ReadState,
) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return "", false, err
	}

	if s.closed {
		return "", false, ErrClosed
	}

	if s.stopped || s.driver.Stopped() {
		return "", false, ErrStopped
	}

	if request == nil {
		return "", false, ErrInvalidReadConfirmation
	}

	status := s.node.Status()

	if status.Role != raft.Leader ||
		status.Term != request.term {
		return "", false, raft.ErrNotLeader
	}

	if s.pendingReads[string(request.context)] != request {
		return "", false, ErrInvalidReadConfirmation
	}

	if state.RequestID == 0 ||
		state.Term != request.term ||
		state.Index == 0 ||
		state.Index > status.Commit ||
		!bytes.Equal(state.Context, request.context) {
		return "", false, ErrInvalidReadConfirmation
	}

	accepted := request.confirmation

	if accepted == nil ||
		accepted.RequestID != state.RequestID ||
		accepted.Term != state.Term ||
		accepted.Index != state.Index ||
		!bytes.Equal(accepted.Context, state.Context) {
		return "", false, ErrInvalidReadConfirmation
	}

	if s.store.AppliedIndex() < state.Index {
		return "", false, ErrInvalidReadConfirmation
	}

	value, found := s.store.Get(key)

	return value, found, nil
}
