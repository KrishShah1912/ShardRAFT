package raft

import (
	"errors"
	"testing"
)

func newReadTrackingLeader(t *testing.T) *Raft {
	t.Helper()

	r := NewRaft(1, nil, 5)

	for i := 0; i < 30; i++ {
		r.Tick()

		if r.Status().Role == Leader {
			r.Advance()
			return r
		}
	}

	t.Fatal("node did not become leader")
	return nil
}

func TestPendingReadRegistration(t *testing.T) {
	r := newReadTrackingLeader(t)

	index := r.Status().Commit

	request, err := r.registerPendingRead(
		[]byte("read-1"),
		index,
	)

	if err != nil {
		t.Fatal(err)
	}

	if request.id == 0 {
		t.Fatal("zero read request ID")
	}

	if request.term != r.Status().Term {
		t.Fatal("incorrect read term")
	}

	if request.index != index {
		t.Fatal("incorrect read index")
	}

	if r.pendingReadCount() != 1 {
		t.Fatal("incorrect pending read count")
	}

	if _, ok := request.acks[r.id]; !ok {
		t.Fatal("missing leader acknowledgment")
	}

	if len(request.acks) != 1 {
		t.Fatal("unexpected acknowledgments")
	}
}

func TestPendingReadUniqueIDs(t *testing.T) {
	r := newReadTrackingLeader(t)

	var previous uint64

	for i := 0; i < 100; i++ {
		request, err := r.registerPendingRead(
			[]byte("same-context"),
			r.Status().Commit,
		)

		if err != nil {
			t.Fatal(err)
		}

		if request.id <= previous {
			t.Fatal("read ID was reused")
		}

		previous = request.id
	}

	if r.pendingReadCount() != 100 {
		t.Fatal("incorrect pending read count")
	}
}

func TestPendingReadContextCopy(t *testing.T) {
	r := newReadTrackingLeader(t)

	ctx := []byte("original")

	request, err := r.registerPendingRead(
		ctx,
		r.Status().Commit,
	)

	if err != nil {
		t.Fatal(err)
	}

	ctx[0] = 'X'

	if string(request.context) != "original" {
		t.Fatal("caller modified stored context")
	}
}

func TestPendingReadRejectNonLeader(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 5)

	_, err := r.registerPendingRead(
		[]byte("follower-read"),
		0,
	)

	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected error: %v", err)
	}

	r.startElection()

	_, err = r.registerPendingRead(
		[]byte("candidate-read"),
		0,
	)

	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("nonleader registered a read")
	}
}

func TestPendingReadInvalidContext(t *testing.T) {
	r := newReadTrackingLeader(t)

	contexts := [][]byte{
		nil,
		{},
		make([]byte, maxReadContextSize+1),
	}

	for _, ctx := range contexts {
		_, err := r.registerPendingRead(
			ctx,
			r.Status().Commit,
		)

		if !errors.Is(err, ErrInvalidReadContext) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("invalid request was registered")
	}
}

func TestPendingReadCapacity(t *testing.T) {
	r := newReadTrackingLeader(t)

	for i := 0; i < maxPendingReads; i++ {
		_, err := r.registerPendingRead(
			[]byte("read"),
			r.Status().Commit,
		)

		if err != nil {
			t.Fatal(err)
		}
	}

	_, err := r.registerPendingRead(
		[]byte("overflow"),
		r.Status().Commit,
	)

	if !errors.Is(err, ErrTooManyPendingReads) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount() != maxPendingReads {
		t.Fatal("pending capacity exceeded")
	}

	r.removePendingRead(1)

	_, err = r.registerPendingRead(
		[]byte("replacement"),
		r.Status().Commit,
	)

	if err != nil {
		t.Fatal(err)
	}
}

func TestPendingReadRemoval(t *testing.T) {
	r := newReadTrackingLeader(t)

	request, err := r.registerPendingRead(
		[]byte("remove"),
		r.Status().Commit,
	)

	if err != nil {
		t.Fatal(err)
	}

	r.removePendingRead(request.id)
	r.removePendingRead(request.id)

	if r.pendingReadCount() != 0 {
		t.Fatal("request was not removed")
	}
}

func TestPendingReadClear(t *testing.T) {
	r := newReadTrackingLeader(t)

	for i := 0; i < 10; i++ {
		_, err := r.registerPendingRead(
			[]byte("read"),
			r.Status().Commit,
		)

		if err != nil {
			t.Fatal(err)
		}
	}

	previousID := r.nextReadID

	r.clearPendingReads()

	if r.pendingReadCount() != 0 {
		t.Fatal("pending reads were not cleared")
	}

	request, err := r.registerPendingRead(
		[]byte("new-read"),
		r.Status().Commit,
	)

	if err != nil {
		t.Fatal(err)
	}

	if request.id <= previousID {
		t.Fatal("request ID reused after cleanup")
	}
}

func TestPendingReadIDExhaustion(t *testing.T) {
	r := newReadTrackingLeader(t)

	r.nextReadID = ^uint64(0)

	_, err := r.registerPendingRead(
		[]byte("read"),
		r.Status().Commit,
	)

	if !errors.Is(err, ErrReadIDExhausted) {
		t.Fatalf("unexpected error: %v", err)
	}

	if r.pendingReadCount() != 0 {
		t.Fatal("exhausted ID was registered")
	}
}

func TestPendingReadDoesNotAppendLog(t *testing.T) {
	r := newReadTrackingLeader(t)

	before := r.lastIndex()
	commit := r.Status().Commit

	for i := 0; i < 100; i++ {
		_, err := r.registerPendingRead(
			[]byte("read"),
			commit,
		)

		if err != nil {
			t.Fatal(err)
		}
	}

	if r.lastIndex() != before {
		t.Fatal("read tracking appended log entries")
	}

	if r.Status().Commit != commit {
		t.Fatal("read tracking changed commit index")
	}

	if len(r.Ready().Entries) != 0 {
		t.Fatal("read tracking generated durable entries")
	}
}
