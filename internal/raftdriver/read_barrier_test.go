package raftdriver

import (
	"errors"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func readBarrierTestBatch(
	index uint64,
) raft.Ready {
	return raft.Ready{
		ReadStates: []raft.ReadState{
			{
				RequestID: 1,
				Term:      1,
				Index:     index,
				Context:   []byte("barrier"),
			},
		},
	}
}

func newReadBarrierTestDriver(
	t *testing.T,
	index uint64,
	applied *uint64,
) (*Driver, *readStateTestNode, *[]raft.ReadState) {
	t.Helper()

	driver, node, _ :=
		newReadStateTestDriver(
			t,
			readBarrierTestBatch(index),
			"",
		)

	delivered := &[]raft.ReadState{}

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 {
			return *applied
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			*delivered = append(
				*delivered,
				state,
			)

			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	return driver, node, delivered
}

func TestDriverReadBarrierBehind(t *testing.T) {
	applied := uint64(7)

	driver, node, delivered :=
		newReadBarrierTestDriver(
			t, 10, &applied,
		)

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 0 ||
		len(node.acknowledged) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("unsatisfied barrier was delivered")
	}

	if driver.Stopped() {
		t.Fatal("waiting for application stopped driver")
	}
}

func TestDriverReadBarrierEqual(t *testing.T) {
	applied := uint64(10)

	driver, node, delivered :=
		newReadBarrierTestDriver(
			t, 10, &applied,
		)

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 1 ||
		len(node.acknowledged) != 1 ||
		len(node.ready.ReadStates) != 0 {
		t.Fatal("satisfied barrier was not delivered")
	}
}

func TestDriverReadBarrierAhead(t *testing.T) {
	applied := uint64(12)

	driver, node, delivered :=
		newReadBarrierTestDriver(
			t, 10, &applied,
		)

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 1 ||
		len(node.acknowledged) != 1 {
		t.Fatal("advanced state machine rejected read")
	}

	if (*delivered)[0].Index != 10 {
		t.Fatal("captured barrier index changed")
	}
}

func TestDriverReadBarrierDeferredDelivery(t *testing.T) {
	applied := uint64(5)

	driver, node, delivered :=
		newReadBarrierTestDriver(
			t, 10, &applied,
		)

	for applied < 10 {
		if err := driver.Process(); err != nil {
			t.Fatal(err)
		}

		if len(*delivered) != 0 ||
			len(node.ready.ReadStates) != 1 {
			t.Fatal("read delivered prematurely")
		}

		applied++
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 1 ||
		len(node.ready.ReadStates) != 0 {
		t.Fatal("deferred read was not delivered")
	}

	for i := 0; i < 10; i++ {
		if err := driver.Process(); err != nil {
			t.Fatal(err)
		}
	}

	if len(*delivered) != 1 {
		t.Fatal("deferred read delivered twice")
	}
}

func TestDriverReadBarrierMissingProvider(t *testing.T) {
	applied := uint64(10)

	driver, node, delivered :=
		newReadBarrierTestDriver(
			t, 10, &applied,
		)

	if err := driver.SetReadStateAppliedIndex(nil); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 0 ||
		len(node.acknowledged) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("missing provider permitted delivery")
	}
}

func TestDriverReadBarrierLateProvider(t *testing.T) {
	applied := uint64(10)

	driver, node, delivered :=
		newReadBarrierTestDriver(
			t, 10, &applied,
		)

	if err := driver.SetReadStateAppliedIndex(nil); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 0 {
		t.Fatal("read delivered without provider")
	}

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return applied },
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 1 ||
		len(node.ready.ReadStates) != 0 {
		t.Fatal("late provider failed to release read")
	}
}

func TestDriverReadBarrierZeroIndex(t *testing.T) {
	applied := uint64(100)

	driver, node, delivered :=
		newReadBarrierTestDriver(
			t, 0, &applied,
		)

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 0 ||
		len(node.acknowledged) != 0 {
		t.Fatal("invalid zero barrier was delivered")
	}
}

func TestDriverReadBarrierZeroApplied(t *testing.T) {
	applied := uint64(0)

	driver, node, delivered :=
		newReadBarrierTestDriver(
			t, 1, &applied,
		)

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(*delivered) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("unapplied state machine delivered read")
	}
}

func TestDriverReadBarrierIndependentRequests(t *testing.T) {
	applied := uint64(5)

	ready := raft.Ready{
		ReadStates: []raft.ReadState{
			{
				RequestID: 1,
				Term:      1,
				Index:     10,
				Context:   []byte("later"),
			},
			{
				RequestID: 2,
				Term:      1,
				Index:     3,
				Context:   []byte("earlier"),
			},
			{
				RequestID: 3,
				Term:      1,
				Index:     5,
				Context:   []byte("current"),
			},
		},
	}

	driver, node, _ :=
		newReadStateTestDriver(t, ready, "")

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return applied },
	); err != nil {
		t.Fatal(err)
	}

	var delivered []uint64

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			delivered = append(
				delivered,
				state.RequestID,
			)

			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		delivered,
		[]uint64{2, 3},
	) {
		t.Fatalf("incorrect delivery: %v", delivered)
	}

	if len(node.ready.ReadStates) != 1 ||
		node.ready.ReadStates[0].RequestID != 1 {
		t.Fatal("unsatisfied request was discarded")
	}

	applied = 10

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(
		delivered,
		[]uint64{2, 3, 1},
	) {
		t.Fatalf("incorrect final delivery: %v", delivered)
	}
}

func TestDriverReadBarrierCommitNotApplication(t *testing.T) {
	ready := readStateTestBatch()

	applied := uint64(0)

	driver, node, _ :=
		newReadStateTestDriver(t, ready, "")

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return applied },
	); err != nil {
		t.Fatal(err)
	}

	delivered := 0

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			delivered++
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if delivered != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("commit was incorrectly treated as application")
	}

	applied = 1

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if delivered != 1 {
		t.Fatal("applied barrier was not released")
	}
}

func TestDriverReadBarrierApplicationFailure(t *testing.T) {
	driver, node, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"apply",
		)

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return 100 },
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("failed application delivered read")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected application failure")
	}

	if len(node.acknowledged) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("failed application lost confirmation")
	}

	if !driver.Stopped() {
		t.Fatal("driver did not stop")
	}
}

func TestDriverReadBarrierStoppedProvider(t *testing.T) {
	driver, _, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"sync",
		)

	if err := driver.Process(); err == nil {
		t.Fatal("expected sync failure")
	}

	err := driver.SetReadStateAppliedIndex(
		func() uint64 { return 100 },
	)

	if !errors.Is(err, ErrStopped) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDriverReadBarrierActualRaft(t *testing.T) {
	node := raft.NewRaft(1, nil, 5)

	events := []string{}

	storage := &readStateTestStorage{
		events: &events,
	}

	var applied uint64

	driver, err := New(
		node,
		storage,
		func(entry raft.Entry) error {
			applied = entry.Index
			return nil
		},
		func(messages []raft.Message) error {
			return nil
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return applied },
	); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		if node.Status().Role == raft.Leader {
			break
		}

		node.Tick()

		if err := driver.Process(); err != nil {
			t.Fatal(err)
		}
	}

	if node.Status().Role != raft.Leader {
		t.Fatal("single-node election failed")
	}

	var delivered []raft.ReadState

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			if applied < state.Index {
				t.Fatal("read delivered before application")
			}

			delivered = append(
				delivered,
				state,
			)

			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	previous := applied

	if err := node.Propose(
		[]byte("new-command"),
	); err != nil {
		t.Fatal(err)
	}

	if err := node.ReadIndex(
		[]byte("after-command"),
	); err != nil {
		t.Fatal(err)
	}

	states := node.Ready().ReadStates

	if len(states) != 1 {
		t.Fatal("missing quorum confirmation")
	}

	if states[0].Index <= previous {
		t.Fatal("test did not create an unapplied barrier")
	}

	if len(delivered) != 0 {
		t.Fatal("premature delivery")
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(delivered) != 1 ||
		applied < delivered[0].Index ||
		len(node.Ready().ReadStates) != 0 {
		t.Fatal("actual Raft barrier was not processed")
	}
}

func TestDriverReadBarrierLeadershipLoss(t *testing.T) {
	node := raft.NewRaft(1, nil, 5)

	events := []string{}

	storage := &readStateTestStorage{
		events: &events,
	}

	driver, err := New(
		node,
		storage,
		func(entry raft.Entry) error {
			return nil
		},
		func(messages []raft.Message) error {
			return nil
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		if node.Status().Role == raft.Leader {
			break
		}

		node.Tick()

		if err := driver.Process(); err != nil {
			t.Fatal(err)
		}
	}

	if node.Status().Role != raft.Leader {
		t.Fatal("single-node election failed")
	}

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return 0 },
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("obsolete confirmation delivered")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := node.ReadIndex(
		[]byte("deferred"),
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(node.Ready().ReadStates) != 1 {
		t.Fatal("deferred confirmation missing")
	}

	if err := node.Step(raft.Message{
		Type: raft.MsgHeartbeat,
		From: 2,
		To:   1,
		Term: node.Status().Term + 1,
	}); err != nil {
		t.Fatal(err)
	}

	if node.Status().Role == raft.Leader {
		t.Fatal("node did not step down")
	}

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return 100 },
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(node.Ready().ReadStates) != 0 {
		t.Fatal("obsolete confirmation survived")
	}
}
