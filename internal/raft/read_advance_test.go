package raft

import (
	"reflect"
	"testing"
)

func TestReadAdvanceSingleConfirmation(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "single")

	states := r.Ready().ReadStates

	if len(states) != 1 {
		t.Fatal("missing confirmation")
	}

	r.AdvanceReadStates(states)

	if len(r.Ready().ReadStates) != 0 {
		t.Fatal("acknowledged confirmation remains")
	}

	if r.completedReadCount() != 0 {
		t.Fatal("completed queue was not cleared")
	}
}

func TestReadAdvanceMultipleConfirmations(t *testing.T) {
	r := newReadReadyLeader(t)

	for i := 0; i < 10; i++ {
		requireReadReadyAdmission(t, r, "multiple")
	}

	states := r.Ready().ReadStates

	if len(states) != 10 {
		t.Fatal("incorrect confirmation count")
	}

	r.AdvanceReadStates(states)

	if r.completedReadCount() != 0 {
		t.Fatal("completed confirmations remain")
	}
}

func TestReadAdvancePartialAcknowledgment(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "first")
	requireReadReadyAdmission(t, r, "second")
	requireReadReadyAdmission(t, r, "third")

	states := r.Ready().ReadStates

	r.AdvanceReadStates(states[1:2])

	remaining := r.Ready().ReadStates

	if len(remaining) != 2 {
		t.Fatal("incorrect remaining confirmation count")
	}

	if remaining[0].RequestID != states[0].RequestID ||
		remaining[1].RequestID != states[2].RequestID {
		t.Fatal("unacknowledged confirmations changed")
	}
}

func TestReadAdvanceLaterCompletionPreserved(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "first")

	first := r.Ready().ReadStates

	requireReadReadyAdmission(t, r, "later")

	r.AdvanceReadStates(first)

	remaining := r.Ready().ReadStates

	if len(remaining) != 1 {
		t.Fatal("later confirmation was lost")
	}

	if string(remaining[0].Context) != "later" {
		t.Fatal("incorrect remaining confirmation")
	}
}

func TestReadAdvanceDuplicateAcknowledgment(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "duplicate")

	states := r.Ready().ReadStates

	r.AdvanceReadStates(states)
	r.AdvanceReadStates(states)
	r.AdvanceReadStates(states)

	if r.completedReadCount() != 0 {
		t.Fatal("duplicate acknowledgment changed queue")
	}
}

func TestReadAdvanceUnknownRequest(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "existing")

	before := r.Ready().ReadStates

	r.AdvanceReadStates([]ReadState{
		{
			RequestID: before[0].RequestID + 100,
			Term:      before[0].Term,
		},
	})

	if !reflect.DeepEqual(
		before,
		r.Ready().ReadStates,
	) {
		t.Fatal("unknown acknowledgment modified queue")
	}
}

func TestReadAdvanceWrongTerm(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "current")

	before := r.Ready().ReadStates

	wrong := before[0]
	wrong.Term++

	r.AdvanceReadStates([]ReadState{wrong})

	if !reflect.DeepEqual(
		before,
		r.Ready().ReadStates,
	) {
		t.Fatal("wrong-term acknowledgment removed read")
	}
}

func TestReadAdvanceInvalidIdentity(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "valid")

	before := r.Ready().ReadStates

	r.AdvanceReadStates([]ReadState{
		{},
		{
			RequestID: before[0].RequestID,
			Term:      0,
		},
		{
			RequestID: 0,
			Term:      before[0].Term,
		},
	})

	if !reflect.DeepEqual(
		before,
		r.Ready().ReadStates,
	) {
		t.Fatal("invalid identity modified queue")
	}
}

func TestReadAdvanceEmptyAcknowledgment(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "preserved")

	before := r.Ready().ReadStates

	r.AdvanceReadStates(nil)
	r.AdvanceReadStates([]ReadState{})

	if !reflect.DeepEqual(
		before,
		r.Ready().ReadStates,
	) {
		t.Fatal("empty acknowledgment modified queue")
	}
}

func TestReadAdvancePendingRequestPreserved(t *testing.T) {
	r := newReadAPILeader(
		t,
		[]NodeID{2, 3},
	)

	first := startAutomaticRead(t, r, "first")

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, first, 2),
	)

	states := r.Ready().ReadStates

	if len(states) != 1 {
		t.Fatal("missing first confirmation")
	}

	second := startAutomaticRead(t, r, "pending")

	r.AdvanceReadStates(states)

	if r.pendingReadCount() != 1 {
		t.Fatal("pending request was removed")
	}

	if r.pendingReads[second.id] != second {
		t.Fatal("pending request identity changed")
	}

	stepReadHeartbeatAck(
		t,
		r,
		readHeartbeatAck(r, second, 2),
	)

	remaining := r.Ready().ReadStates

	if len(remaining) != 1 ||
		remaining[0].RequestID != second.id {
		t.Fatal("pending request failed to complete")
	}
}

func TestReadAdvanceCapacityRecovery(t *testing.T) {
	r := newReadReadyLeader(t)

	for i := 0; i < maxPendingReads; i++ {
		requireReadReadyAdmission(t, r, "capacity")
	}

	if err := r.ReadIndex([]byte("overflow")); err != ErrTooManyPendingReads {
		t.Fatalf("expected capacity error, got %v", err)
	}

	states := r.Ready().ReadStates

	r.AdvanceReadStates(states[:1])

	requireReadReadyAdmission(t, r, "replacement")

	if r.completedReadCount() != maxPendingReads {
		t.Fatal("capacity was not recovered")
	}
}

func TestReadAdvanceLegacyAdvancePreservesReads(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "preserved")

	before := r.Ready().ReadStates

	r.Advance()

	if !reflect.DeepEqual(
		before,
		r.Ready().ReadStates,
	) {
		t.Fatal("legacy Advance discarded read states")
	}

	r.AdvanceReadStates(before)

	if r.completedReadCount() != 0 {
		t.Fatal("explicit acknowledgment failed")
	}
}

func TestReadAdvanceReturnedCopyPreserved(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "copied")

	states := r.Ready().ReadStates
	expected := cloneReadStates(states)

	r.AdvanceReadStates(states)

	if !reflect.DeepEqual(states, expected) {
		t.Fatal("acknowledgment modified caller copy")
	}
}

func TestReadAdvanceLeadershipInvalidation(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "obsolete")

	old := r.Ready().ReadStates

	r.becomeFollower(r.hard.Term+1, 2)

	r.AdvanceReadStates(old)

	if r.completedReadCount() != 0 {
		t.Fatal("obsolete confirmation survived")
	}

	r.startElection()

	if r.volatile.Role != Leader {
		t.Fatal("single-node election failed")
	}

	requireReadReadyAdmission(t, r, "new-term")

	current := r.Ready().ReadStates

	r.AdvanceReadStates(old)

	if !reflect.DeepEqual(
		current,
		r.Ready().ReadStates,
	) {
		t.Fatal("old acknowledgment removed new confirmation")
	}
}

func TestReadAdvanceNoDurableChanges(t *testing.T) {
	r := newReadReadyLeader(t)

	hard := r.hard
	log := cloneEntries(r.log)
	stable := cloneEntries(r.stableLog)
	applied := r.volatile.AppliedIndex

	for i := 0; i < 100; i++ {
		requireReadReadyAdmission(t, r, "read")

		r.AdvanceReadStates(r.Ready().ReadStates)
	}

	if r.hard != hard ||
		!reflect.DeepEqual(log, r.log) ||
		!reflect.DeepEqual(stable, r.stableLog) ||
		r.volatile.AppliedIndex != applied {
		t.Fatal("read acknowledgment modified durable state")
	}

	ready := r.Ready()

	if ready.HardState != nil ||
		ready.TruncateFrom != 0 ||
		len(ready.Entries) != 0 ||
		len(ready.CommittedEntries) != 0 ||
		len(ready.Messages) != 0 ||
		len(ready.ReadStates) != 0 {
		t.Fatal("unexpected Ready work")
	}
}
