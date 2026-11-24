package raftdriver

import (
	"errors"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestDriverReadSafetySendFailure(t *testing.T) {
	driver, node, events := newReadStateTestDriver(
		t, readStateTestBatch(), "send",
	)

	delivered := 0

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			delivered++
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected transport failure")
	}

	if delivered != 0 ||
		len(node.acknowledged) != 0 {
		t.Fatal("failed transport delivered read")
	}

	if !driver.Stopped() {
		t.Fatal("driver did not stop")
	}

	if node.advances != 1 {
		t.Fatal("persisted Ready was not advanced")
	}

	if len(node.ready.ReadStates) != 1 {
		t.Fatal("failed transport discarded confirmation")
	}

	expected := []string{
		"append",
		"hard-state",
		"sync",
		"apply",
		"advance",
		"send",
	}

	if !reflect.DeepEqual(*events, expected) {
		t.Fatalf("unexpected processing order: %v", *events)
	}
}

func TestDriverReadSafetyStoppedAfterSendFailure(t *testing.T) {
	driver, node, _ := newReadStateTestDriver(
		t, readStateTestBatch(), "send",
	)

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("stopped driver delivered read")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected send failure")
	}

	for i := 0; i < 10; i++ {
		if err := driver.Process(); !errors.Is(err, ErrStopped) {
			t.Fatalf("unexpected stopped result: %v", err)
		}
	}

	if len(node.acknowledged) != 0 {
		t.Fatal("stopped driver acknowledged read")
	}
}

func TestDriverReadSafetySuccessfulSend(t *testing.T) {
	driver, node, events := newReadStateTestDriver(
		t, readStateTestBatch(), "",
	)

	delivered := 0

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			if len(*events) == 0 ||
				(*events)[len(*events)-1] != "send" {
				t.Fatal("read delivered before successful send")
			}

			delivered++
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if delivered != 1 ||
		len(node.acknowledged) != 1 {
		t.Fatal("successful send did not release read")
	}
}

func TestDriverReadSafetyInvalidIdentity(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*raft.ReadState)
	}{
		{
			name: "zero-request-id",
			mutate: func(state *raft.ReadState) {
				state.RequestID = 0
			},
		},
		{
			name: "zero-term",
			mutate: func(state *raft.ReadState) {
				state.Term = 0
			},
		},
		{
			name: "empty-context",
			mutate: func(state *raft.ReadState) {
				state.Context = nil
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ready := readStateTestBatch()

			tc.mutate(&ready.ReadStates[0])

			driver, node, _ := newReadStateTestDriver(
				t, ready, "",
			)

			if err := driver.SetReadStateHandler(
				func(state raft.ReadState) error {
					t.Fatal("invalid identity delivered")
					return nil
				},
			); err != nil {
				t.Fatal(err)
			}

			if err := driver.Process(); err == nil {
				t.Fatal("expected invalid identity error")
			}

			if !driver.Stopped() ||
				len(node.acknowledged) != 0 {
				t.Fatal("invalid identity was acknowledged")
			}
		})
	}
}

func TestDriverReadSafetyDuplicateIdentity(t *testing.T) {
	ready := readStateTestBatch()

	ready.ReadStates = append(
		ready.ReadStates,
		ready.ReadStates[0],
	)

	driver, node, _ := newReadStateTestDriver(
		t, ready, "",
	)

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("duplicate batch reached consumer")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected duplicate identity error")
	}

	if !driver.Stopped() ||
		len(node.acknowledged) != 0 {
		t.Fatal("duplicate identity was acknowledged")
	}
}

func TestDriverReadSafetyMalformedBatchAtomicValidation(t *testing.T) {
	ready := readStateTestBatch()

	ready.ReadStates = append(
		ready.ReadStates,
		raft.ReadState{
			RequestID: 2,
			Term:      0,
			Index:     1,
			Context:   []byte("invalid"),
		},
	)

	driver, node, _ := newReadStateTestDriver(
		t, ready, "",
	)

	delivered := 0

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			delivered++
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected malformed batch error")
	}

	if delivered != 0 ||
		len(node.acknowledged) != 0 {
		t.Fatal("malformed batch was partially delivered")
	}
}

func TestDriverReadSafetyDistinctIdentities(t *testing.T) {
	ready := readStateTestBatch()

	ready.ReadStates = append(
		ready.ReadStates,
		raft.ReadState{
			RequestID: 2,
			Term:      1,
			Index:     1,
			Context:   []byte("first"),
		},
	)

	driver, node, _ := newReadStateTestDriver(
		t, ready, "",
	)

	var delivered []uint64

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			delivered = append(
				delivered, state.RequestID,
			)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(delivered, []uint64{1, 2}) ||
		len(node.acknowledged) != 2 {
		t.Fatal("distinct requests were incorrectly rejected")
	}
}

func TestDriverReadSafetyBarrierAfterSend(t *testing.T) {
	ready := readStateTestBatch()
	ready.ReadStates[0].Index = 5

	driver, node, _ := newReadStateTestDriver(
		t, ready, "",
	)

	applied := uint64(1)
	delivered := 0

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 {
			return applied
		},
	); err != nil {
		t.Fatal(err)
	}

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
		t.Fatal("successful send bypassed application barrier")
	}

	applied = 5

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if delivered != 1 ||
		len(node.acknowledged) != 1 {
		t.Fatal("satisfied barrier was not delivered")
	}
}
