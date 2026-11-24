package raft

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func newReadCancelLeader(t *testing.T) *Raft {
	t.Helper()

	return newReadAPILeader(
		t,
		[]NodeID{2, 3},
	)
}

func submitReadCancelRequest(
	t *testing.T,
	r *Raft,
	ctx []byte,
) uint64 {
	t.Helper()

	if err := r.ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}

	return r.nextReadID
}

func TestReadCancelPending(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("cancel-me")

	id := submitReadCancelRequest(t, r, ctx)

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("expected successful cancellation")
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("pending request was not removed")
	}

	if _, exists := r.pendingReads[id]; exists {
		t.Fatal("canceled request still exists")
	}

	if r.completedReadCount() != 0 {
		t.Fatal("cancellation produced confirmation")
	}
}

func TestReadCancelUnknown(t *testing.T) {
	r := newReadCancelLeader(t)

	submitReadCancelRequest(t, r, []byte("active"))

	if r.CancelReadIndex(
		r.hard.Term,
		[]byte("unknown"),
	) {
		t.Fatal("unknown cancellation succeeded")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("unrelated request was removed")
	}
}

func TestReadCancelWrongTerm(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("active")

	submitReadCancelRequest(t, r, ctx)

	if r.CancelReadIndex(r.hard.Term+1, ctx) {
		t.Fatal("wrong-term cancellation succeeded")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("wrong term removed active request")
	}
}

func TestReadCancelZeroTerm(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("active")

	submitReadCancelRequest(t, r, ctx)

	if r.CancelReadIndex(0, ctx) {
		t.Fatal("zero-term cancellation succeeded")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("active request was removed")
	}
}

func TestReadCancelInvalidContext(t *testing.T) {
	r := newReadCancelLeader(t)

	submitReadCancelRequest(t, r, []byte("active"))

	contexts := [][]byte{
		nil,
		{},
		make([]byte, maxReadContextSize+1),
	}

	for _, ctx := range contexts {
		if r.CancelReadIndex(r.hard.Term, ctx) {
			t.Fatal("invalid cancellation succeeded")
		}
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("invalid cancellation removed request")
	}
}

func TestReadCancelIdempotent(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("cancel-once")

	submitReadCancelRequest(t, r, ctx)

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("initial cancellation failed")
	}

	for i := 0; i < 100; i++ {
		if r.CancelReadIndex(r.hard.Term, ctx) {
			t.Fatal("repeated cancellation succeeded")
		}
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("canceled request reappeared")
	}
}

func TestReadCancelIndependentRequests(t *testing.T) {
	r := newReadCancelLeader(t)

	first := submitReadCancelRequest(
		t, r, []byte("first"),
	)

	second := submitReadCancelRequest(
		t, r, []byte("second"),
	)

	third := submitReadCancelRequest(
		t, r, []byte("third"),
	)

	if !r.CancelReadIndex(
		r.hard.Term,
		[]byte("second"),
	) {
		t.Fatal("cancellation failed")
	}

	if r.pendingReadCount() != 2 {
		t.Fatal("unexpected pending request count")
	}

	if r.pendingReads[first] == nil ||
		r.pendingReads[third] == nil {
		t.Fatal("unrelated request removed")
	}

	if r.pendingReads[second] != nil {
		t.Fatal("target request was preserved")
	}
}

func TestReadCancelDuplicateContext(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("shared-context")

	submitReadCancelRequest(t, r, ctx)
	submitReadCancelRequest(t, r, ctx)

	submitReadCancelRequest(
		t, r, []byte("independent"),
	)

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("matching contexts were not removed")
	}

	for _, request := range r.pendingReads {
		if string(request.context) != "independent" {
			t.Fatal("wrong request preserved")
		}
	}
}

func TestReadCancelBinaryContext(t *testing.T) {
	r := newReadCancelLeader(t)

	ctx := []byte{0, 1, 0, 255, 128, 0}

	submitReadCancelRequest(t, r, ctx)

	if !r.CancelReadIndex(
		r.hard.Term,
		[]byte{0, 1, 0, 255, 128, 0},
	) {
		t.Fatal("binary context cancellation failed")
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("binary request remains pending")
	}
}

func TestReadCancelCopiedContext(t *testing.T) {
	r := newReadCancelLeader(t)

	ctx := []byte("original")

	submitReadCancelRequest(t, r, ctx)

	ctx[0] = 'X'

	if r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("mutated context matched request")
	}

	if !r.CancelReadIndex(
		r.hard.Term,
		[]byte("original"),
	) {
		t.Fatal("original context did not match")
	}
}

func TestReadCancelFollower(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 5)

	if r.CancelReadIndex(1, []byte("unknown")) {
		t.Fatal("follower canceled nonexistent request")
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("follower acquired pending reads")
	}
}

func TestReadCancelAfterLeadershipLoss(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("old-leader")
	term := r.hard.Term

	submitReadCancelRequest(t, r, ctx)

	r.becomeFollower(term+1, 2)

	if r.CancelReadIndex(term, ctx) {
		t.Fatal("invalidated request still existed")
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 {
		t.Fatal("obsolete reads remain")
	}
}

func TestReadCancelMonotonicIDs(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("reused-context")

	first := submitReadCancelRequest(t, r, ctx)

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	second := submitReadCancelRequest(t, r, ctx)

	if second <= first {
		t.Fatal("read request ID was reused")
	}

	if r.pendingReads[second] == nil {
		t.Fatal("replacement request missing")
	}
}

func TestReadCancelLateAcknowledgment(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("late-response")

	oldID := submitReadCancelRequest(t, r, ctx)

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	newID := submitReadCancelRequest(t, r, ctx)

	if err := r.Step(Message{
		Type:      MsgHeartbeatResp,
		From:      2,
		To:        r.id,
		Term:      r.hard.Term,
		RequestID: oldID,
	}); err != nil {
		t.Fatal(err)
	}

	if r.completedReadCount() != 0 {
		t.Fatal("late response completed canceled read")
	}

	request := r.pendingReads[newID]

	if request == nil || len(request.acks) != 1 {
		t.Fatal("late response affected replacement read")
	}

	if err := r.Step(Message{
		Type:      MsgHeartbeatResp,
		From:      2,
		To:        r.id,
		Term:      r.hard.Term,
		RequestID: newID,
	}); err != nil {
		t.Fatal(err)
	}

	states := r.Ready().ReadStates

	if len(states) != 1 ||
		states[0].RequestID != newID {
		t.Fatal("replacement read did not complete")
	}
}

func TestReadCancelNoDurableChanges(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("volatile-only")

	submitReadCancelRequest(t, r, ctx)

	beforeHard := r.hard
	beforeStableHard := r.stableHard
	beforeLog := cloneEntries(r.log)
	beforeStableLog := cloneEntries(r.stableLog)
	beforeStatus := r.Status()
	beforeID := r.nextReadID
	beforeVolatile := r.volatile

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	if r.hard != beforeHard ||
		r.stableHard != beforeStableHard ||
		r.Status() != beforeStatus ||
		r.volatile != beforeVolatile ||
		r.nextReadID != beforeID ||
		!reflect.DeepEqual(beforeLog, r.log) ||
		!reflect.DeepEqual(beforeStableLog, r.stableLog) {
		t.Fatal("cancellation changed consensus state")
	}

	ready := r.Ready()

	if ready.HardState != nil ||
		len(ready.Entries) != 0 ||
		len(ready.CommittedEntries) != 0 ||
		ready.TruncateFrom != 0 {
		t.Fatal("cancellation produced persistent work")
	}
}

func TestReadCancelCapacityRecovery(t *testing.T) {
	r := newReadCancelLeader(t)

	for i := 0; i < maxPendingReads; i++ {
		submitReadCancelRequest(
			t,
			r,
			[]byte(fmt.Sprintf("read-%d", i)),
		)

		// Simulate delivery of outgoing messages.
		r.Advance()
	}

	if err := r.ReadIndex([]byte("overflow")); !errors.Is(err, ErrTooManyPendingReads) {
		t.Fatalf("unexpected capacity error: %v", err)
	}

	if !r.CancelReadIndex(
		r.hard.Term,
		[]byte("read-0"),
	) {
		t.Fatal("capacity cancellation failed")
	}

	submitReadCancelRequest(
		t, r, []byte("replacement"),
	)

	if r.pendingReadCount() != maxPendingReads {
		t.Fatal("capacity was not recovered")
	}
}

func TestReadCancelRepeatedCapacityRecovery(t *testing.T) {
	r := newReadCancelLeader(t)

	for i := 0; i < maxPendingReads*2; i++ {
		ctx := []byte(fmt.Sprintf("read-%d", i))

		submitReadCancelRequest(t, r, ctx)

		if !r.CancelReadIndex(r.hard.Term, ctx) {
			t.Fatalf("cancellation failed at %d", i)
		}

		r.Advance()

		if r.pendingReadCount() != 0 {
			t.Fatalf("pending request leaked at %d", i)
		}
	}

	if r.nextReadID != uint64(maxPendingReads*2) {
		t.Fatal("request ID sequence is incorrect")
	}
}
