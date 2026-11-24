package raft

import (
	"bytes"
	"reflect"
	"testing"
)

func newReadReadyLeader(t *testing.T) *Raft {
	t.Helper()

	r := newReadTrackingLeader(t)

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("leader has no current-term commit")
	}

	return r
}

func requireReadReadyAdmission(
	t *testing.T,
	r *Raft,
	context string,
) {
	t.Helper()

	if err := r.ReadIndex([]byte(context)); err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}
}

func TestReadReadyEmpty(t *testing.T) {
	r := newReadReadyLeader(t)

	if states := r.Ready().ReadStates; states != nil {
		t.Fatalf("expected nil read states, got %+v", states)
	}
}

func TestReadReadySingleNode(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "single-read")

	states := r.Ready().ReadStates

	if len(states) != 1 {
		t.Fatalf("expected one read state, got %d", len(states))
	}

	state := states[0]

	if state.RequestID == 0 {
		t.Fatal("invalid request ID")
	}

	if state.Term != r.hard.Term {
		t.Fatal("incorrect read term")
	}

	if state.Index != r.hard.Commit {
		t.Fatal("incorrect read index")
	}

	if string(state.Context) != "single-read" {
		t.Fatal("incorrect read context")
	}
}

func TestReadReadyRepeatedInspection(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "repeat")

	first := r.Ready().ReadStates
	second := r.Ready().ReadStates

	if len(first) != 1 || len(second) != 1 {
		t.Fatal("Ready unexpectedly consumed read states")
	}

	if !reflect.DeepEqual(first, second) {
		t.Fatal("repeated Ready results differ")
	}

	if r.completedReadCount() != 1 {
		t.Fatal("completed queue was modified")
	}
}

func TestReadReadyContextIsolation(t *testing.T) {
	r := newReadReadyLeader(t)

	ctx := []byte("original")

	if err := r.ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}

	ctx[0] = 'X'

	first := r.Ready().ReadStates

	if len(first) != 1 {
		t.Fatal("missing read state")
	}

	first[0].Context[0] = 'Y'

	second := r.Ready().ReadStates

	if len(second) != 1 {
		t.Fatal("missing second read state")
	}

	if string(second[0].Context) != "original" {
		t.Fatal("Ready context aliases mutable memory")
	}
}

func TestReadReadyStateIsolation(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "isolated")

	first := r.Ready().ReadStates

	if len(first) != 1 {
		t.Fatal("missing read state")
	}

	expected := first[0]

	first[0] = ReadState{}

	second := r.Ready().ReadStates

	if len(second) != 1 {
		t.Fatal("missing second read state")
	}

	if !reflect.DeepEqual(expected, second[0]) {
		t.Fatal("Ready slice aliases internal state")
	}
}

func TestReadReadyMultipleRequests(t *testing.T) {
	r := newReadReadyLeader(t)

	contexts := []string{
		"first",
		"second",
		"third",
		"fourth",
	}

	for _, ctx := range contexts {
		requireReadReadyAdmission(t, r, ctx)
	}

	states := r.Ready().ReadStates

	if len(states) != len(contexts) {
		t.Fatalf("expected %d states, got %d",
			len(contexts), len(states))
	}

	seen := make(map[uint64]bool)

	for i, state := range states {
		if state.RequestID == 0 || seen[state.RequestID] {
			t.Fatal("invalid or duplicate request ID")
		}

		seen[state.RequestID] = true

		if string(state.Context) != contexts[i] {
			t.Fatal("incorrect completion order")
		}
	}
}

func TestReadReadyDuplicateContexts(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "same")
	requireReadReadyAdmission(t, r, "same")

	states := r.Ready().ReadStates

	if len(states) != 2 {
		t.Fatal("duplicate contexts lost a request")
	}

	if states[0].RequestID == states[1].RequestID {
		t.Fatal("distinct requests share an ID")
	}

	if !bytes.Equal(states[0].Context, states[1].Context) {
		t.Fatal("unexpected context mismatch")
	}
}

func TestReadReadyCapturedIndex(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "before-proposal")

	index := r.hard.Commit

	if err := r.Propose([]byte("new-command")); err != nil {
		t.Fatal(err)
	}

	if r.hard.Commit <= index {
		t.Fatal("proposal did not advance commit")
	}

	states := r.Ready().ReadStates

	if len(states) != 1 {
		t.Fatal("missing read state")
	}

	if states[0].Index != index {
		t.Fatal("captured read index changed")
	}
}

func TestReadReadyPendingNotExposed(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(t, leader, followers[2])

	requireReadReadyAdmission(t, leader, "pending")

	if leader.pendingReadCount() != 1 {
		t.Fatal("expected one pending read")
	}

	if len(leader.Ready().ReadStates) != 0 {
		t.Fatal("unconfirmed read exposed through Ready")
	}
}

func TestReadReadyQuorumCompletion(t *testing.T) {
	leader, followers :=
		newReadHeartbeatIntegrationCluster(t, 3)

	commitReadIntegrationNoop(t, leader, followers[2])

	requireReadReadyAdmission(t, leader, "confirmed")

	var probe Message

	for _, msg := range leader.Ready().Messages {
		if msg.Type == MsgHeartbeat &&
			msg.RequestID != 0 &&
			msg.To == 2 {
			probe = msg
			break
		}
	}

	if probe.RequestID == 0 {
		t.Fatal("missing read heartbeat")
	}

	if len(leader.Ready().ReadStates) != 0 {
		t.Fatal("read exposed before quorum")
	}

	reply := deliverReadHeartbeatRequest(
		t, followers[2], probe,
	)

	if err := leader.Step(reply); err != nil {
		t.Fatal(err)
	}

	states := leader.Ready().ReadStates

	if len(states) != 1 {
		t.Fatal("quorum-confirmed state not exposed")
	}

	if states[0].RequestID != probe.RequestID {
		t.Fatal("incorrect request correlation")
	}

	if string(states[0].Context) != "confirmed" {
		t.Fatal("incorrect read context")
	}

	if leader.pendingReadCount() != 0 {
		t.Fatal("completed request remains pending")
	}
}

func TestReadReadyLeadershipInvalidation(t *testing.T) {
	r := newReadReadyLeader(t)

	requireReadReadyAdmission(t, r, "obsolete")

	if len(r.Ready().ReadStates) != 1 {
		t.Fatal("missing completed read")
	}

	r.becomeFollower(r.hard.Term+1, 2)

	if len(r.Ready().ReadStates) != 0 {
		t.Fatal("obsolete read survived leadership loss")
	}
}

func TestReadReadyNoDurableChanges(t *testing.T) {
	r := newReadReadyLeader(t)

	hard := r.hard
	log := cloneEntries(r.log)
	index := r.lastIndex()

	requireReadReadyAdmission(t, r, "read-only")

	ready := r.Ready()

	if len(ready.ReadStates) != 1 {
		t.Fatal("missing read state")
	}

	if ready.HardState != nil ||
		ready.TruncateFrom != 0 ||
		len(ready.Entries) != 0 ||
		len(ready.CommittedEntries) != 0 ||
		len(ready.Messages) != 0 {
		t.Fatal("read generated unexpected Ready work")
	}

	if r.hard != hard ||
		r.lastIndex() != index ||
		!reflect.DeepEqual(r.log, log) {
		t.Fatal("read modified durable Raft state")
	}
}

func TestReadReadyWithCommittedEntries(t *testing.T) {
	r := newReadReadyLeader(t)

	if err := r.Propose([]byte("command")); err != nil {
		t.Fatal(err)
	}

	requireReadReadyAdmission(t, r, "after-command")

	ready := r.Ready()

	if len(ready.Entries) != 1 {
		t.Fatal("missing unstable log entry")
	}

	if len(ready.CommittedEntries) != 1 {
		t.Fatal("missing committed entry")
	}

	if ready.HardState == nil {
		t.Fatal("missing hard state")
	}

	if len(ready.ReadStates) != 1 {
		t.Fatal("missing read confirmation")
	}

	if ready.ReadStates[0].Index !=
		ready.CommittedEntries[0].Index {
		t.Fatal("incorrect read barrier index")
	}
}

func TestReadReadyCapacity(t *testing.T) {
	r := newReadReadyLeader(t)

	for i := 0; i < maxPendingReads; i++ {
		requireReadReadyAdmission(t, r, "capacity")
	}

	states := r.Ready().ReadStates

	if len(states) != maxPendingReads {
		t.Fatalf("incorrect capacity: %d", len(states))
	}

	if err := r.ReadIndex([]byte("overflow")); err != ErrTooManyPendingReads {
		t.Fatalf("expected capacity rejection, got %v", err)
	}

	if len(r.Ready().ReadStates) != maxPendingReads {
		t.Fatal("capacity rejection modified completed queue")
	}
}
