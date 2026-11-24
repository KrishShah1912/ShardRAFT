package raft

import (
	"reflect"
	"testing"
)

func newReadCompletionLeader(t *testing.T) *Raft {
	t.Helper()

	r := newReadTrackingLeader(t)

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("leader has no current-term commit")
	}

	return r
}

func registerReadCompletionRequest(
	t *testing.T,
	r *Raft,
	ctx string,
) *pendingRead {
	t.Helper()

	index, err := r.readCommitBarrier()
	if err != nil {
		t.Fatal(err)
	}

	request, err := r.registerPendingRead(
		[]byte(ctx),
		index,
	)
	if err != nil {
		t.Fatal(err)
	}

	r.sendReadHeartbeats(request)

	return request
}

func TestReadCompletionSingleNode(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "single-node",
	)

	if !r.completePendingRead(request) {
		t.Fatal("single-node read did not complete")
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("completed request remains pending")
	}

	if r.completedReadCount() != 1 {
		t.Fatal("expected one completed read")
	}

	state := r.completedReads[0]

	if state.RequestID != request.id ||
		state.Term != request.term ||
		state.Index != request.index ||
		string(state.Context) != "single-node" {
		t.Fatalf("incorrect read state: %+v", state)
	}
}

func TestReadCompletionDuplicate(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "duplicate",
	)

	if !r.completePendingRead(request) {
		t.Fatal("initial completion failed")
	}

	for i := 0; i < 100; i++ {
		if r.completePendingRead(request) {
			t.Fatal("duplicate completion accepted")
		}
	}

	if r.completedReadCount() != 1 {
		t.Fatal("duplicate completion was recorded")
	}
}

func TestReadCompletionWithoutQuorum(t *testing.T) {
	r := newReadHeartbeatLeader(
		t, []NodeID{2, 3},
	)

	// Establish the current-term commit independently.
	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()

	request := registerReadCompletionRequest(
		t, r, "without-quorum",
	)

	if r.completePendingRead(request) {
		t.Fatal("read completed without quorum")
	}

	if r.pendingReadCount() != 1 ||
		r.completedReadCount() != 0 {
		t.Fatal("unexpected read lifecycle state")
	}
}

func TestReadCompletionWithoutCurrentTermCommit(t *testing.T) {
	r := newReadHeartbeatLeader(
		t, []NodeID{2, 3},
	)

	request, err := r.registerPendingRead(
		[]byte("without-commit"),
		r.lastIndex(),
	)
	if err != nil {
		t.Fatal(err)
	}

	r.sendReadHeartbeats(request)

	request.acks[2] = struct{}{}

	if !r.hasReadQuorum(request) {
		t.Fatal("expected quorum confirmation")
	}

	if r.hasCommittedCurrentTerm() {
		t.Fatal("unexpected current-term commit")
	}

	if r.completePendingRead(request) {
		t.Fatal("read completed without current-term commit")
	}
}

func TestReadCompletionBothConditions(t *testing.T) {
	r := newReadHeartbeatLeader(
		t, []NodeID{2, 3},
	)

	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()

	request := registerReadCompletionRequest(
		t, r, "both-conditions",
	)

	request.acks[2] = struct{}{}

	if !r.completePendingRead(request) {
		t.Fatal("valid quorum-confirmed read did not complete")
	}

	if r.completedReadCount() != 1 ||
		r.pendingReadCount() != 0 {
		t.Fatal("incorrect completion state")
	}
}

func TestReadCompletionLeadershipLoss(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "leadership-loss",
	)

	r.becomeFollower(r.hard.Term+1, 0)

	if r.completePendingRead(request) {
		t.Fatal("read completed after leadership loss")
	}

	if r.completedReadCount() != 0 {
		t.Fatal("unexpected completed read")
	}
}

func TestReadCompletionStaleTerm(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "stale-term",
	)

	request.term++

	if r.completePendingRead(request) {
		t.Fatal("wrong-term request completed")
	}
}

func TestReadCompletionRemovedRequest(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "removed",
	)

	r.removePendingRead(request.id)

	if r.completePendingRead(request) {
		t.Fatal("removed request completed")
	}
}

func TestReadCompletionUnstartedRequest(t *testing.T) {
	r := newReadCompletionLeader(t)

	request, err := r.registerPendingRead(
		[]byte("unstarted"),
		r.hard.Commit,
	)
	if err != nil {
		t.Fatal(err)
	}

	if r.completePendingRead(request) {
		t.Fatal("unstarted request completed")
	}
}

func TestReadCompletionInvalidIndex(t *testing.T) {
	for _, index := range []uint64{0, ^uint64(0)} {
		r := newReadCompletionLeader(t)

		request, err := r.registerPendingRead(
			[]byte("invalid-index"),
			index,
		)
		if err != nil {
			t.Fatal(err)
		}

		r.sendReadHeartbeats(request)

		if r.completePendingRead(request) {
			t.Fatalf("invalid index %d accepted", index)
		}
	}
}

func TestReadCompletionNilRequest(t *testing.T) {
	r := newReadCompletionLeader(t)

	if r.completePendingRead(nil) {
		t.Fatal("nil request completed")
	}
}

func TestReadCompletionContextIsolation(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "original-context",
	)

	if !r.completePendingRead(request) {
		t.Fatal("completion failed")
	}

	request.context[0] = 'X'

	if string(r.completedReads[0].Context) !=
		"original-context" {
		t.Fatal("completed context aliases pending request")
	}

	copied := cloneReadStates(r.completedReads)

	copied[0].Context[0] = 'Y'
	copied[0].Index++

	if string(r.completedReads[0].Context) !=
		"original-context" {
		t.Fatal("cloned context aliases internal state")
	}

	if copied[0].Index == r.completedReads[0].Index {
		t.Fatal("cloned state aliases internal state")
	}
}

func TestReadCompletionCapturedIndex(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "captured-index",
	)

	captured := request.index

	if err := r.Propose([]byte("new-command")); err != nil {
		t.Fatal(err)
	}

	if r.hard.Commit <= captured {
		t.Fatal("commit index did not advance")
	}

	if !r.completePendingRead(request) {
		t.Fatal("completion failed")
	}

	if r.completedReads[0].Index != captured {
		t.Fatal("captured read index was changed")
	}
}

func TestReadCompletionConcurrentRequests(t *testing.T) {
	r := newReadCompletionLeader(t)

	first := registerReadCompletionRequest(
		t, r, "first",
	)

	second := registerReadCompletionRequest(
		t, r, "second",
	)

	if !r.completePendingRead(second) {
		t.Fatal("second completion failed")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("first request was removed")
	}

	if !r.completePendingRead(first) {
		t.Fatal("first completion failed")
	}

	if r.completedReadCount() != 2 {
		t.Fatal("expected two completed reads")
	}

	if r.completedReads[0].RequestID != second.id ||
		r.completedReads[1].RequestID != first.id {
		t.Fatal("read identities were mixed")
	}
}

func TestReadCompletionClear(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "clear",
	)

	if !r.completePendingRead(request) {
		t.Fatal("completion failed")
	}

	r.clearCompletedReads()
	r.clearCompletedReads()

	if r.completedReadCount() != 0 {
		t.Fatal("completed reads were not cleared")
	}

	if r.nextReadID != request.id {
		t.Fatal("request ID sequence was reset")
	}
}

func TestReadCompletionCapacity(t *testing.T) {
	r := newReadCompletionLeader(t)

	for i := 0; i < maxPendingReads; i++ {
		request := registerReadCompletionRequest(
			t, r, "capacity",
		)

		if !r.completePendingRead(request) {
			t.Fatalf("completion %d failed", i)
		}
	}

	request := registerReadCompletionRequest(
		t, r, "overflow",
	)

	if r.completePendingRead(request) {
		t.Fatal("completion capacity exceeded")
	}

	if r.completedReadCount() != maxPendingReads ||
		r.pendingReadCount() != 1 {
		t.Fatal("incorrect bounded queue state")
	}

	r.clearCompletedReads()

	if !r.completePendingRead(request) {
		t.Fatal("completion did not recover after clearing")
	}
}

func TestReadCompletionNoDurableChanges(t *testing.T) {
	r := newReadCompletionLeader(t)

	request := registerReadCompletionRequest(
		t, r, "no-durable-changes",
	)

	beforeLog := cloneEntries(r.log)
	beforeHard := r.hard
	beforeVolatile := r.volatile
	beforeSnapshot := r.SnapshotMetadata()
	beforeAppendID := r.nextRequestID

	if !r.completePendingRead(request) {
		t.Fatal("completion failed")
	}

	if !reflect.DeepEqual(beforeLog, r.log) {
		t.Fatal("completion modified the log")
	}

	if beforeHard != r.hard ||
		beforeVolatile != r.volatile {
		t.Fatal("completion modified consensus state")
	}

	if beforeSnapshot != r.SnapshotMetadata() {
		t.Fatal("completion modified snapshot metadata")
	}

	if beforeAppendID != r.nextRequestID {
		t.Fatal("completion modified append request IDs")
	}

	rd := r.Ready()

	if rd.HardState != nil ||
		rd.TruncateFrom != 0 ||
		len(rd.Entries) != 0 ||
		len(rd.CommittedEntries) != 0 {
		t.Fatal("completion generated durable work")
	}
}
