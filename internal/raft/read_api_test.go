package raft

import (
	"errors"
	"reflect"
	"testing"
)

func newReadAPILeader(
	t *testing.T,
	peers []NodeID,
) *Raft {
	t.Helper()

	r := newReadHeartbeatLeader(t, peers)

	if len(peers) > 0 {
		for _, peer := range peers {
			r.matchIndex[peer] = r.lastIndex()
		}
		r.maybeCommit()
	}

	if !r.hasCommittedCurrentTerm() {
		t.Fatal("expected current-term commit")
	}

	r.Advance()

	return r
}

func TestReadAPIFollower(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 5)

	err := r.ReadIndex([]byte("follower"))

	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 ||
		r.nextReadID != 0 ||
		len(r.messages) != 0 {
		t.Fatal("rejected read modified request state")
	}
}

func TestReadAPICandidate(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 5)

	r.startElection()
	r.Advance()

	err := r.ReadIndex([]byte("candidate"))

	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 {
		t.Fatal("candidate accepted a read")
	}
}

func TestReadAPIInvalidContext(t *testing.T) {
	r := newReadAPILeader(t, nil)

	contexts := [][]byte{
		nil,
		{},
		make([]byte, maxReadContextSize+1),
	}

	for _, ctx := range contexts {
		err := r.ReadIndex(ctx)

		if !errors.Is(err, ErrInvalidReadContext) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if r.nextReadID != 0 ||
		r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 ||
		len(r.messages) != 0 {
		t.Fatal("invalid context modified request state")
	}
}

func TestReadAPIMaximumContext(t *testing.T) {
	r := newReadAPILeader(t, nil)

	ctx := make([]byte, maxReadContextSize)

	if err := r.ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}

	if r.completedReadCount() != 1 {
		t.Fatal("maximum-sized context was not accepted")
	}

	if len(r.completedReads[0].Context) !=
		maxReadContextSize {
		t.Fatal("context length changed")
	}
}

func TestReadAPIUncommittedCurrentTerm(t *testing.T) {
	r := newReadHeartbeatLeader(
		t, []NodeID{2, 3},
	)

	err := r.ReadIndex([]byte("not-ready"))

	if !errors.Is(err, ErrReadNotReady) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.nextReadID != 0 ||
		r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 ||
		len(r.messages) != 0 {
		t.Fatal("unready read modified request state")
	}
}

func TestReadAPIRetryAfterCommit(t *testing.T) {
	r := newReadHeartbeatLeader(
		t, []NodeID{2, 3},
	)

	ctx := []byte("retry-after-commit")

	if err := r.ReadIndex(ctx); !errors.Is(
		err, ErrReadNotReady,
	) {
		t.Fatalf("unexpected initial error: %v", err)
	}

	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()
	r.Advance()

	if err := r.ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}

	if r.pendingReadCount() != 1 ||
		r.nextReadID != 1 {
		t.Fatal("retry did not register exactly one read")
	}
}

func TestReadAPISingleNode(t *testing.T) {
	r := newReadAPILeader(t, nil)

	index := r.hard.Commit

	if err := r.ReadIndex([]byte("single")); err != nil {
		t.Fatal(err)
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 1 {
		t.Fatal("single-node confirmation failed")
	}

	state := r.completedReads[0]

	if state.RequestID != 1 ||
		state.Term != r.hard.Term ||
		state.Index != index ||
		string(state.Context) != "single" {
		t.Fatalf("incorrect read state: %+v", state)
	}

	if len(r.messages) != 0 {
		t.Fatal("single-node read generated network traffic")
	}
}

func TestReadAPIThreeNodes(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3},
	)

	if err := r.ReadIndex([]byte("three")); err != nil {
		t.Fatal(err)
	}

	if r.pendingReadCount() != 1 ||
		r.completedReadCount() != 0 {
		t.Fatal("read completed before remote confirmation")
	}

	request := r.pendingReads[1]

	if request == nil ||
		!request.started ||
		request.index != r.hard.Commit ||
		request.term != r.hard.Term {
		t.Fatal("incorrect pending request")
	}

	messages := r.Ready().Messages

	if len(messages) != 2 {
		t.Fatalf("expected two probes, got %d", len(messages))
	}

	for _, msg := range messages {
		if msg.Type != MsgHeartbeat ||
			msg.RequestID != request.id ||
			msg.Term != request.term ||
			msg.Index != 0 ||
			msg.Commit != 0 ||
			len(msg.Entries) != 0 {
			t.Fatalf("invalid read heartbeat: %+v", msg)
		}
	}
}

func TestReadAPIFiveNodes(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3, 4, 5},
	)

	if err := r.ReadIndex([]byte("five")); err != nil {
		t.Fatal(err)
	}

	if len(r.Ready().Messages) != 4 {
		t.Fatal("expected four read heartbeat probes")
	}

	if r.completedReadCount() != 0 {
		t.Fatal("read completed without remote quorum")
	}
}

func TestReadAPIContextCopy(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3},
	)

	ctx := []byte("original")

	if err := r.ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}

	ctx[0] = 'X'

	if string(r.pendingReads[1].context) != "original" {
		t.Fatal("pending read aliases caller context")
	}
}

func TestReadAPIConcurrentRequests(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3},
	)

	for i := 0; i < 10; i++ {
		if err := r.ReadIndex([]byte("same-context")); err != nil {
			t.Fatal(err)
		}
	}

	if r.pendingReadCount() != 10 ||
		r.completedReadCount() != 0 ||
		r.nextReadID != 10 {
		t.Fatal("incorrect concurrent request state")
	}

	for id := uint64(1); id <= 10; id++ {
		request := r.pendingReads[id]

		if request == nil || request.id != id {
			t.Fatal("request identity missing or reused")
		}
	}

	if len(r.Ready().Messages) != 20 {
		t.Fatal("incorrect number of correlated probes")
	}
}

func TestReadAPICapturedIndex(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3},
	)

	firstIndex := r.hard.Commit

	if err := r.ReadIndex([]byte("first")); err != nil {
		t.Fatal(err)
	}

	if err := r.Propose([]byte("command")); err != nil {
		t.Fatal(err)
	}

	r.matchIndex[2] = r.lastIndex()
	r.maybeCommit()

	if err := r.ReadIndex([]byte("second")); err != nil {
		t.Fatal(err)
	}

	if r.pendingReads[1].index != firstIndex {
		t.Fatal("first captured index changed")
	}

	if r.pendingReads[2].index != r.hard.Commit ||
		r.pendingReads[2].index <= firstIndex {
		t.Fatal("second request captured incorrect index")
	}
}

func TestReadAPIPendingCapacity(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3},
	)

	for i := 0; i < maxPendingReads; i++ {
		if err := r.ReadIndex([]byte("pending")); err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}

	beforeID := r.nextReadID
	beforeMessages := len(r.messages)

	err := r.ReadIndex([]byte("overflow"))

	if !errors.Is(err, ErrTooManyPendingReads) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount() != maxPendingReads ||
		r.nextReadID != beforeID ||
		len(r.messages) != beforeMessages {
		t.Fatal("capacity rejection modified state")
	}
}

func TestReadAPICompletedCapacity(t *testing.T) {
	r := newReadAPILeader(t, nil)

	for i := 0; i < maxPendingReads; i++ {
		if err := r.ReadIndex([]byte("completed")); err != nil {
			t.Fatal(err)
		}
	}

	err := r.ReadIndex([]byte("overflow"))

	if !errors.Is(err, ErrTooManyPendingReads) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.completedReadCount() != maxPendingReads ||
		r.pendingReadCount() != 0 {
		t.Fatal("completed queue capacity exceeded")
	}

	r.clearCompletedReads()

	if err := r.ReadIndex([]byte("recovered")); err != nil {
		t.Fatal(err)
	}

	if r.completedReadCount() != 1 ||
		r.nextReadID != uint64(maxPendingReads)+1 {
		t.Fatal("capacity did not recover correctly")
	}
}

func TestReadAPICombinedCapacity(t *testing.T) {
	r := newReadAPILeader(
		t, []NodeID{2, 3},
	)

	for i := 0; i < maxPendingReads/2; i++ {
		if err := r.ReadIndex([]byte("completed")); err != nil {
			t.Fatal(err)
		}

		request := r.pendingReads[r.nextReadID]
		request.acks[2] = struct{}{}

		if !r.completePendingRead(request) {
			t.Fatal("completion failed")
		}
	}

	for i := maxPendingReads / 2; i < maxPendingReads; i++ {
		if err := r.ReadIndex([]byte("pending")); err != nil {
			t.Fatal(err)
		}
	}

	err := r.ReadIndex([]byte("overflow"))

	if !errors.Is(err, ErrTooManyPendingReads) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount()+r.completedReadCount() !=
		maxPendingReads {
		t.Fatal("combined lifecycle capacity exceeded")
	}
}

func TestReadAPIIDExhaustion(t *testing.T) {
	r := newReadAPILeader(t, nil)

	r.nextReadID = ^uint64(0)

	err := r.ReadIndex([]byte("exhausted"))

	if !errors.Is(err, ErrReadIDExhausted) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 ||
		r.nextReadID != ^uint64(0) {
		t.Fatal("request ID exhaustion corrupted state")
	}
}

func TestReadAPINoDurableChanges(t *testing.T) {
	r := newReadAPILeader(t, nil)

	beforeLog := cloneEntries(r.log)
	beforeHard := r.hard
	beforeVolatile := r.volatile
	beforeSnapshot := r.SnapshotMetadata()
	beforeAppendID := r.nextRequestID

	for i := 0; i < 100; i++ {
		if err := r.ReadIndex([]byte("read-only")); err != nil {
			t.Fatal(err)
		}
	}

	if !reflect.DeepEqual(beforeLog, r.log) ||
		beforeHard != r.hard ||
		beforeVolatile != r.volatile ||
		beforeSnapshot != r.SnapshotMetadata() ||
		beforeAppendID != r.nextRequestID {
		t.Fatal("ReadIndex modified consensus state")
	}

	rd := r.Ready()

	if rd.HardState != nil ||
		rd.TruncateFrom != 0 ||
		len(rd.Entries) != 0 ||
		len(rd.CommittedEntries) != 0 {
		t.Fatal("ReadIndex generated durable work")
	}
}

func TestReadAPILeadershipLoss(t *testing.T) {
	r := newReadAPILeader(t, nil)

	r.becomeFollower(r.hard.Term+1, 0)
	r.Advance()

	err := r.ReadIndex([]byte("lost-leadership"))

	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount() != 0 ||
		r.completedReadCount() != 0 {
		t.Fatal("former leader accepted a new read")
	}
}
