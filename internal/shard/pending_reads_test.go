package shard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func registerTestShardRead(
	t *testing.T,
	s *Shard,
	term uint64,
) *pendingShardRead {
	t.Helper()

	request, err := s.registerPendingRead(term)
	if err != nil {
		t.Fatal(err)
	}

	return request
}

func TestShardPendingReadInitialState(t *testing.T) {
	s := &Shard{}

	if s.pendingReadCount() != 0 {
		t.Fatal("unexpected initial pending reads")
	}

	if s.nextReadID != 0 {
		t.Fatal("unexpected initial request ID")
	}
}

func TestShardPendingReadRegistration(t *testing.T) {
	s := &Shard{}

	request := registerTestShardRead(t, s, 7)

	if request.id != 1 || request.term != 7 {
		t.Fatalf("unexpected request: %+v", request)
	}

	if len(request.context) != 16 {
		t.Fatal("unexpected context size")
	}

	if cap(request.done) != 1 {
		t.Fatal("completion channel must be buffered")
	}

	if s.pendingReadCount() != 1 {
		t.Fatal("request was not registered")
	}

	if s.pendingReads[string(request.context)] != request {
		t.Fatal("request lookup failed")
	}
}

func TestShardPendingReadContextEncoding(t *testing.T) {
	s := &Shard{}

	request := registerTestShardRead(t, s, 42)

	term := binary.BigEndian.Uint64(request.context[:8])
	id := binary.BigEndian.Uint64(request.context[8:])

	if term != 42 || id != request.id {
		t.Fatalf("unexpected context: term=%d id=%d", term, id)
	}
}

func TestShardPendingReadUniqueIDs(t *testing.T) {
	s := &Shard{}
	seen := make(map[string]bool)

	for i := 0; i < 100; i++ {
		request := registerTestShardRead(t, s, 1)

		if request.id != uint64(i+1) {
			t.Fatal("request IDs are not monotonic")
		}

		key := string(request.context)

		if seen[key] {
			t.Fatal("duplicate read context")
		}

		seen[key] = true
	}

	if s.pendingReadCount() != 100 {
		t.Fatal("unexpected pending request count")
	}
}

func TestShardPendingReadDistinctTerms(t *testing.T) {
	s := &Shard{}

	first := registerTestShardRead(t, s, 1)
	second := registerTestShardRead(t, s, 2)

	if bytes.Equal(first.context, second.context) {
		t.Fatal("contexts must be distinct")
	}

	if second.id <= first.id {
		t.Fatal("request ID was reused")
	}
}

func TestShardPendingReadInvalidTerm(t *testing.T) {
	s := &Shard{}

	request, err := s.registerPendingRead(0)

	if request != nil || !errors.Is(err, ErrInvalidReadTerm) {
		t.Fatalf("unexpected result: %v, %v", request, err)
	}

	if s.pendingReadCount() != 0 || s.nextReadID != 0 {
		t.Fatal("invalid registration modified state")
	}
}

func TestShardPendingReadRemoval(t *testing.T) {
	s := &Shard{}

	first := registerTestShardRead(t, s, 1)
	second := registerTestShardRead(t, s, 1)

	if !s.removePendingRead(first) {
		t.Fatal("failed to remove request")
	}

	if s.pendingReadCount() != 1 {
		t.Fatal("unexpected pending count")
	}

	if s.pendingReads[string(second.context)] != second {
		t.Fatal("unrelated request was removed")
	}

	if s.removePendingRead(first) {
		t.Fatal("duplicate removal succeeded")
	}

	if !s.removePendingRead(second) {
		t.Fatal("failed to remove second request")
	}

	if s.pendingReads != nil {
		t.Fatal("empty request map was not released")
	}
}

func TestShardPendingReadUnknownRemoval(t *testing.T) {
	s := &Shard{}

	request := registerTestShardRead(t, s, 1)

	unknown := &pendingShardRead{
		id:      request.id,
		term:    request.term,
		context: append([]byte(nil), request.context...),
	}

	if s.removePendingRead(unknown) {
		t.Fatal("unknown request removed active request")
	}

	if s.removePendingRead(nil) {
		t.Fatal("nil removal succeeded")
	}

	if s.pendingReadCount() != 1 {
		t.Fatal("active request was lost")
	}
}

func TestShardPendingReadCapacity(t *testing.T) {
	s := &Shard{}

	for i := 0; i < maxPendingShardReads; i++ {
		registerTestShardRead(t, s, 1)
	}

	before := s.nextReadID

	request, err := s.registerPendingRead(1)

	if request != nil ||
		!errors.Is(err, ErrTooManyPendingReads) {
		t.Fatalf("unexpected capacity result: %v", err)
	}

	if s.pendingReadCount() != maxPendingShardReads {
		t.Fatal("capacity limit was exceeded")
	}

	if s.nextReadID != before {
		t.Fatal("failed registration consumed an ID")
	}
}

func TestShardPendingReadCapacityRecovery(t *testing.T) {
	s := &Shard{}

	first := registerTestShardRead(t, s, 1)

	for i := 1; i < maxPendingShardReads; i++ {
		registerTestShardRead(t, s, 1)
	}

	if !s.removePendingRead(first) {
		t.Fatal("failed to release capacity")
	}

	next := registerTestShardRead(t, s, 1)

	if next.id != uint64(maxPendingShardReads+1) {
		t.Fatal("request identity was reused")
	}

	if s.pendingReadCount() != maxPendingShardReads {
		t.Fatal("unexpected recovered capacity")
	}
}

func TestShardPendingReadIDExhaustion(t *testing.T) {
	s := &Shard{
		nextReadID: ^uint64(0) - 1,
	}

	last := registerTestShardRead(t, s, 1)

	if last.id != ^uint64(0) {
		t.Fatal("unexpected final request ID")
	}

	request, err := s.registerPendingRead(1)

	if request != nil ||
		!errors.Is(err, ErrReadIDExhausted) {
		t.Fatalf("unexpected exhaustion result: %v", err)
	}

	if s.nextReadID != ^uint64(0) {
		t.Fatal("request ID wrapped")
	}
}

func TestShardPendingReadFailAll(t *testing.T) {
	s := &Shard{}

	requests := make([]*pendingShardRead, 100)

	for i := range requests {
		requests[i] = registerTestShardRead(t, s, 1)
	}

	s.failPendingReads(ErrClosed)

	if s.pendingReadCount() != 0 || s.pendingReads != nil {
		t.Fatal("failed requests were retained")
	}

	for _, request := range requests {
		select {
		case result := <-request.done:
			if !errors.Is(result.err, ErrClosed) {
				t.Fatalf("unexpected failure: %v", result.err)
			}
		default:
			t.Fatal("request was not notified")
		}
	}
}

func TestShardPendingReadFailureOverridesConfirmation(t *testing.T) {
	s := &Shard{}

	request := registerTestShardRead(t, s, 1)

	request.done <- shardReadResult{
		state: raft.ReadState{
			RequestID: 1,
			Term:      1,
			Index:     1,
			Context:   request.context,
		},
	}

	s.failPendingReads(ErrStopped)

	select {
	case result := <-request.done:
		if !errors.Is(result.err, ErrStopped) {
			t.Fatal("terminal failure did not replace confirmation")
		}

		if result.state.Index != 0 {
			t.Fatal("obsolete confirmation was retained")
		}
	default:
		t.Fatal("terminal failure was not delivered")
	}
}

func TestShardPendingReadFailureIdempotent(t *testing.T) {
	s := &Shard{}

	request := registerTestShardRead(t, s, 1)

	s.failPendingReads(ErrClosed)
	s.failPendingReads(ErrStopped)

	result := <-request.done

	if !errors.Is(result.err, ErrClosed) {
		t.Fatal("original terminal failure changed")
	}

	select {
	case <-request.done:
		t.Fatal("duplicate failure notification")
	default:
	}
}

func TestShardPendingReadNilFailure(t *testing.T) {
	s := &Shard{}

	request := registerTestShardRead(t, s, 1)

	s.failPendingReads(nil)

	if s.pendingReadCount() != 1 {
		t.Fatal("nil failure removed request")
	}

	select {
	case <-request.done:
		t.Fatal("unexpected notification")
	default:
	}
}

func TestShardPendingReadIdentityAfterFailure(t *testing.T) {
	s := &Shard{}

	first := registerTestShardRead(t, s, 1)

	s.failPendingReads(ErrStopped)

	second := registerTestShardRead(t, s, 2)

	if second.id <= first.id {
		t.Fatal("request ID was reused after cleanup")
	}

	if bytes.Equal(first.context, second.context) {
		t.Fatal("request context was reused")
	}
}
