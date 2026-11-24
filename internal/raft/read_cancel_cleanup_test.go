package raft

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func confirmReadCancelCleanup(
	t *testing.T,
	r *Raft,
	id uint64,
) {
	t.Helper()

	if err := r.Step(Message{
		Type:      MsgHeartbeatResp,
		From:      2,
		To:        r.id,
		Term:      r.hard.Term,
		RequestID: id,
	}); err != nil {
		t.Fatal(err)
	}
}

func countReadCancelProbes(
	r *Raft,
	id uint64,
) int {
	count := 0

	for _, msg := range r.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID == id {
			count++
		}
	}

	return count
}

func TestReadCancelCleanupPendingProbes(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("pending")

	id := submitReadCancelRequest(t, r, ctx)

	if countReadCancelProbes(r, id) != 2 {
		t.Fatal("expected two queued probes")
	}

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	if countReadCancelProbes(r, id) != 0 {
		t.Fatal("canceled probes remain queued")
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("pending request remains")
	}
}

func TestReadCancelCleanupCompleted(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("completed")

	id := submitReadCancelRequest(t, r, ctx)
	confirmReadCancelCleanup(t, r, id)

	if r.completedReadCount() != 1 {
		t.Fatal("expected completed confirmation")
	}

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("completed cancellation failed")
	}

	if len(r.Ready().ReadStates) != 0 {
		t.Fatal("canceled confirmation remains")
	}

	if countReadCancelProbes(r, id) != 0 {
		t.Fatal("completed request probes remain")
	}
}

func TestReadCancelCleanupSingleNode(t *testing.T) {
	r := newReadAPILeader(t, nil)
	ctx := []byte("single-node")

	submitReadCancelRequest(t, r, ctx)

	if r.completedReadCount() != 1 {
		t.Fatal("single-node read did not complete")
	}

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("single-node cancellation failed")
	}

	if r.completedReadCount() != 0 {
		t.Fatal("single-node confirmation remains")
	}
}

func TestReadCancelCleanupIndependentProbes(t *testing.T) {
	r := newReadCancelLeader(t)

	first := submitReadCancelRequest(
		t, r, []byte("first"),
	)

	second := submitReadCancelRequest(
		t, r, []byte("second"),
	)

	if !r.CancelReadIndex(
		r.hard.Term,
		[]byte("first"),
	) {
		t.Fatal("cancellation failed")
	}

	if countReadCancelProbes(r, first) != 0 {
		t.Fatal("canceled probes remain")
	}

	if countReadCancelProbes(r, second) != 2 {
		t.Fatal("unrelated probes were removed")
	}

	if r.pendingReads[second] == nil {
		t.Fatal("unrelated request was removed")
	}
}

func TestReadCancelCleanupIndependentCompleted(t *testing.T) {
	r := newReadCancelLeader(t)

	first := submitReadCancelRequest(
		t, r, []byte("first"),
	)

	second := submitReadCancelRequest(
		t, r, []byte("second"),
	)

	confirmReadCancelCleanup(t, r, first)
	confirmReadCancelCleanup(t, r, second)

	before := r.Ready().ReadStates

	if len(before) != 2 {
		t.Fatal("expected two confirmations")
	}

	if !r.CancelReadIndex(
		r.hard.Term,
		[]byte("first"),
	) {
		t.Fatal("cancellation failed")
	}

	after := r.Ready().ReadStates

	if len(after) != 1 ||
		!reflect.DeepEqual(after[0], before[1]) {
		t.Fatal("unrelated confirmation changed")
	}
}

func TestReadCancelCleanupMixedMatching(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("shared")

	first := submitReadCancelRequest(t, r, ctx)
	second := submitReadCancelRequest(t, r, ctx)

	third := submitReadCancelRequest(
		t, r, []byte("independent"),
	)

	confirmReadCancelCleanup(t, r, first)

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("mixed cancellation failed")
	}

	if r.completedReadCount() != 0 ||
		r.pendingReadCount() != 1 {
		t.Fatal("matching reads were not removed")
	}

	if r.pendingReads[third] == nil {
		t.Fatal("unrelated request missing")
	}

	if countReadCancelProbes(r, first) != 0 ||
		countReadCancelProbes(r, second) != 0 {
		t.Fatal("matching probes remain")
	}
}

func TestReadCancelCleanupPreservesMessages(t *testing.T) {
	r := newReadCancelLeader(t)

	ctx := []byte("cancel")
	id := submitReadCancelRequest(t, r, ctx)

	term := r.hard.Term

	preserved := []Message{
		{
			Type: MsgHeartbeat,
			From: r.id,
			To:   2,
			Term: term,
		},
		{
			Type:      MsgAppend,
			From:      r.id,
			To:        2,
			Term:      term,
			RequestID: id,
		},
		{
			Type:      MsgAppendResp,
			From:      r.id,
			To:        2,
			Term:      term,
			RequestID: id,
		},
		{
			Type:      MsgHeartbeatResp,
			From:      r.id,
			To:        2,
			Term:      term,
			RequestID: id,
		},
		{
			Type: MsgVote,
			From: r.id,
			To:   2,
			Term: term,
		},
		{
			Type: MsgVoteResp,
			From: r.id,
			To:   2,
			Term: term,
		},
	}

	r.messages = append(r.messages, preserved...)

	var expected []Message

	for _, msg := range r.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.From == r.id &&
			msg.Term == term &&
			msg.RequestID == id {
			continue
		}

		expected = append(expected, msg)
	}

	if !r.CancelReadIndex(term, ctx) {
		t.Fatal("cancellation failed")
	}

	if !reflect.DeepEqual(
		r.Ready().Messages,
		expected,
	) {
		t.Fatal("unrelated messages changed")
	}
}

func TestReadCancelCleanupPreservesOtherTerm(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("term-sensitive")

	id := submitReadCancelRequest(t, r, ctx)

	other := Message{
		Type:      MsgHeartbeat,
		From:      r.id,
		To:        2,
		Term:      r.hard.Term + 1,
		RequestID: id,
	}

	r.messages = append(r.messages, other)

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	count := 0

	for _, msg := range r.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID == id {
			if !reflect.DeepEqual(msg, other) {
				t.Fatal("wrong probe preserved")
			}

			count++
		}
	}

	if count != 1 {
		t.Fatal("other-term probe was removed")
	}
}

func TestReadCancelCleanupWrongTermCompleted(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("completed")

	id := submitReadCancelRequest(t, r, ctx)
	confirmReadCancelCleanup(t, r, id)

	before := r.Ready()

	if r.CancelReadIndex(r.hard.Term+1, ctx) {
		t.Fatal("wrong-term cancellation succeeded")
	}

	if !reflect.DeepEqual(before, r.Ready()) {
		t.Fatal("wrong-term cancellation changed Ready")
	}
}

func TestReadCancelCleanupUnknownContext(t *testing.T) {
	r := newReadCancelLeader(t)

	id := submitReadCancelRequest(
		t, r, []byte("completed"),
	)

	confirmReadCancelCleanup(t, r, id)

	submitReadCancelRequest(
		t, r, []byte("pending"),
	)

	before := r.Ready()

	if r.CancelReadIndex(
		r.hard.Term,
		[]byte("unknown"),
	) {
		t.Fatal("unknown cancellation succeeded")
	}

	if !reflect.DeepEqual(before, r.Ready()) {
		t.Fatal("unknown cancellation changed Ready")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("pending request changed")
	}
}

func TestReadCancelCleanupNoRetry(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("no-retry")

	id := submitReadCancelRequest(t, r, ctx)

	r.Advance()

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	for i := 0; i < 100; i++ {
		r.Tick()

		if countReadCancelProbes(r, id) != 0 {
			t.Fatal("canceled request was retried")
		}

		r.Advance()
	}
}

func TestReadCancelCleanupLateResponse(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("late")

	id := submitReadCancelRequest(t, r, ctx)

	r.Advance()

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	for i := 0; i < 100; i++ {
		confirmReadCancelCleanup(t, r, id)
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 {
		t.Fatal("late responses recreated canceled read")
	}
}

func TestReadCancelCleanupAfterAdvance(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("advanced")

	id := submitReadCancelRequest(t, r, ctx)
	confirmReadCancelCleanup(t, r, id)

	r.Advance()

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation after Advance failed")
	}

	if len(r.Ready().ReadStates) != 0 {
		t.Fatal("confirmation remains after cancellation")
	}
}

func TestReadCancelCleanupAfterAcknowledgment(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("acknowledged")

	id := submitReadCancelRequest(t, r, ctx)
	confirmReadCancelCleanup(t, r, id)

	r.Advance()
	r.AdvanceReadStates(r.Ready().ReadStates)

	if r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("acknowledged read was canceled again")
	}
}

func TestReadCancelCleanupStaleAcknowledgment(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("reused")

	oldID := submitReadCancelRequest(t, r, ctx)
	confirmReadCancelCleanup(t, r, oldID)

	oldStates := r.Ready().ReadStates

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	newID := submitReadCancelRequest(t, r, ctx)
	confirmReadCancelCleanup(t, r, newID)

	r.AdvanceReadStates(oldStates)

	states := r.Ready().ReadStates

	if len(states) != 1 ||
		states[0].RequestID != newID {
		t.Fatal("stale acknowledgment removed new read")
	}
}

func TestReadCancelCleanupCompletedCapacity(t *testing.T) {
	r := newReadAPILeader(t, nil)

	for i := 0; i < maxPendingReads; i++ {
		submitReadCancelRequest(
			t,
			r,
			[]byte(fmt.Sprintf("read-%d", i)),
		)
	}

	if err := r.ReadIndex([]byte("overflow")); !errors.Is(err, ErrTooManyPendingReads) {
		t.Fatalf("unexpected capacity error: %v", err)
	}

	if !r.CancelReadIndex(
		r.hard.Term,
		[]byte("read-0"),
	) {
		t.Fatal("completed cancellation failed")
	}

	submitReadCancelRequest(
		t, r, []byte("replacement"),
	)

	if r.completedReadCount() != maxPendingReads {
		t.Fatal("completed capacity was not recovered")
	}
}

func TestReadCancelCleanupRepeatedCompleted(t *testing.T) {
	r := newReadAPILeader(t, nil)

	for i := 0; i < maxPendingReads*2; i++ {
		ctx := []byte(fmt.Sprintf("read-%d", i))

		submitReadCancelRequest(t, r, ctx)

		if !r.CancelReadIndex(r.hard.Term, ctx) {
			t.Fatalf("cancellation failed at %d", i)
		}

		if r.completedReadCount() != 0 ||
			r.pendingReadCount() != 0 {
			t.Fatalf("read leaked at %d", i)
		}
	}
}

func TestReadCancelCleanupNoDurableChanges(t *testing.T) {
	r := newReadCancelLeader(t)
	ctx := []byte("volatile")

	id := submitReadCancelRequest(t, r, ctx)
	confirmReadCancelCleanup(t, r, id)

	beforeHard := r.hard
	beforeStableHard := r.stableHard
	beforeLog := cloneEntries(r.log)
	beforeStableLog := cloneEntries(r.stableLog)
	beforeVolatile := r.volatile
	beforeID := r.nextReadID
	beforeStatus := r.Status()

	if !r.CancelReadIndex(r.hard.Term, ctx) {
		t.Fatal("cancellation failed")
	}

	if r.hard != beforeHard ||
		r.stableHard != beforeStableHard ||
		r.volatile != beforeVolatile ||
		r.nextReadID != beforeID ||
		r.Status() != beforeStatus ||
		!reflect.DeepEqual(beforeLog, r.log) ||
		!reflect.DeepEqual(beforeStableLog, r.stableLog) {
		t.Fatal("cancellation changed consensus state")
	}

	ready := r.Ready()

	if ready.HardState != nil ||
		len(ready.Entries) != 0 ||
		len(ready.CommittedEntries) != 0 ||
		ready.TruncateFrom != 0 {
		t.Fatal("cancellation generated durable work")
	}
}
