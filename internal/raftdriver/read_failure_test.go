package raftdriver

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestDriverReadFailureProcessingBoundaries(t *testing.T) {
	cases := []struct {
		stage    string
		expected []string
		advances int
	}{
		{
			stage: "append",
			expected: []string{
				"append",
			},
		},
		{
			stage: "hard-state",
			expected: []string{
				"append", "hard-state",
			},
		},
		{
			stage: "sync",
			expected: []string{
				"append", "hard-state", "sync",
			},
		},
		{
			stage: "apply",
			expected: []string{
				"append", "hard-state", "sync", "apply",
			},
		},
		{
			stage: "send",
			expected: []string{
				"append", "hard-state", "sync",
				"apply", "advance", "send",
			},
			advances: 1,
		},
		{
			stage: "replace",
			expected: []string{
				"replace",
			},
		},
		{
			stage: "truncate",
			expected: []string{
				"truncate",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.stage, func(t *testing.T) {
			ready := readStateTestBatch()

			if tc.stage == "replace" {
				ready.TruncateFrom = 1
			}

			if tc.stage == "truncate" {
				ready.TruncateFrom = 1
				ready.Entries = nil
			}

			driver, node, events := newReadStateTestDriver(
				t, ready, tc.stage,
			)

			if err := driver.SetReadStateHandler(
				func(state raft.ReadState) error {
					t.Fatal("failed batch delivered confirmation")
					return nil
				},
			); err != nil {
				t.Fatal(err)
			}

			err := driver.Process()

			if err == nil {
				t.Fatal("expected injected failure")
			}

			if !strings.Contains(
				err.Error(),
				"injected "+tc.stage+" failure",
			) {
				t.Fatalf("incorrect failure: %v", err)
			}

			if !reflect.DeepEqual(*events, tc.expected) {
				t.Fatalf(
					"incorrect failure boundary: got %v want %v",
					*events, tc.expected,
				)
			}

			if !driver.Stopped() ||
				node.advances != tc.advances ||
				len(node.acknowledged) != 0 ||
				len(node.ready.ReadStates) != 1 {
				t.Fatal("incorrect failure state")
			}
		})
	}
}

func TestDriverReadFailureMultipleConfirmations(t *testing.T) {
	for _, stage := range []string{
		"append", "hard-state", "sync", "apply", "send",
	} {
		t.Run(stage, func(t *testing.T) {
			ready := readStateTestBatch()

			for id := uint64(2); id <= 10; id++ {
				ready.ReadStates = append(
					ready.ReadStates,
					raft.ReadState{
						RequestID: id,
						Term:      1,
						Index:     1,
						Context:   []byte("pending"),
					},
				)
			}

			driver, node, _ := newReadStateTestDriver(
				t, ready, stage,
			)

			if err := driver.SetReadStateHandler(
				func(state raft.ReadState) error {
					t.Fatal("failed batch delivered confirmation")
					return nil
				},
			); err != nil {
				t.Fatal(err)
			}

			if err := driver.Process(); err == nil {
				t.Fatal("expected failure")
			}

			if len(node.acknowledged) != 0 ||
				len(node.ready.ReadStates) != 10 {
				t.Fatal("failed batch lost confirmations")
			}

			for i, state := range node.ready.ReadStates {
				if state.RequestID != uint64(i+1) {
					t.Fatal("confirmation identity changed")
				}
			}
		})
	}
}

func TestDriverReadFailurePartialApplication(t *testing.T) {
	ready := readStateTestBatch()

	ready.Entries = append(
		ready.Entries,
		raft.Entry{
			Index: 2,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  []byte("second"),
		},
		raft.Entry{
			Index: 3,
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  []byte("third"),
		},
	)

	ready.CommittedEntries = append(
		[]raft.Entry(nil),
		ready.Entries...,
	)

	ready.HardState.Commit = 3

	driver, node, _ := newReadStateTestDriver(
		t, ready, "",
	)

	injected := errors.New("second application failed")

	var applied []uint64

	driver.apply = func(entry raft.Entry) error {
		if entry.Index == 2 {
			return injected
		}

		applied = append(applied, entry.Index)

		return nil
	}

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return 1 },
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("partial application delivered read")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); !errors.Is(err, injected) {
		t.Fatalf("incorrect application error: %v", err)
	}

	if !reflect.DeepEqual(applied, []uint64{1}) {
		t.Fatalf("unexpected applied entries: %v", applied)
	}

	if !driver.Stopped() ||
		node.advances != 0 ||
		len(node.acknowledged) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("incorrect partial application state")
	}
}

func TestDriverReadFailureTransportErrorPropagation(t *testing.T) {
	driver, node, _ := newReadStateTestDriver(
		t, readStateTestBatch(), "",
	)

	injected := errors.New("transport unavailable")

	driver.send = func(messages []raft.Message) error {
		return injected
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("transport failure delivered read")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	err := driver.Process()

	if !errors.Is(err, injected) {
		t.Fatalf("transport error was not preserved: %v", err)
	}

	if !strings.Contains(err.Error(), "send Raft messages") {
		t.Fatalf("missing transport error context: %v", err)
	}

	if !driver.Stopped() ||
		node.advances != 1 ||
		len(node.acknowledged) != 0 {
		t.Fatal("incorrect transport failure state")
	}
}

func TestDriverReadFailureReadOnlySync(t *testing.T) {
	ready := raft.Ready{
		ReadStates: readStateTestBatch().ReadStates,
	}

	driver, node, events := newReadStateTestDriver(
		t, ready, "sync",
	)

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("failed read-only sync delivered read")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected sync failure")
	}

	if !reflect.DeepEqual(*events, []string{"sync"}) {
		t.Fatalf("unexpected operations: %v", *events)
	}

	if !driver.Stopped() ||
		node.advances != 0 ||
		len(node.acknowledged) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("incorrect read-only failure state")
	}
}

func TestDriverReadFailureDeferredConfirmation(t *testing.T) {
	ready := readStateTestBatch()
	ready.ReadStates[0].Index = 5

	driver, node, _ := newReadStateTestDriver(
		t, ready, "",
	)

	applied := uint64(1)

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return applied },
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("failed deferred confirmation delivered")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(node.ready.ReadStates) != 1 {
		t.Fatal("deferred confirmation missing")
	}

	applied = 5

	storage := driver.storage.(*readStateTestStorage)
	storage.failAt = "sync"

	if err := driver.Process(); err == nil {
		t.Fatal("expected deferred sync failure")
	}

	if !driver.Stopped() ||
		len(node.acknowledged) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("deferred confirmation was lost")
	}
}

func TestDriverReadFailureStoppedNoSideEffects(t *testing.T) {
	for _, stage := range []string{
		"append", "hard-state", "sync", "apply", "send",
	} {
		t.Run(stage, func(t *testing.T) {
			driver, node, events := newReadStateTestDriver(
				t, readStateTestBatch(), stage,
			)

			if err := driver.Process(); err == nil {
				t.Fatal("expected initial failure")
			}

			previous := append([]string(nil), (*events)...)
			advances := node.advances

			for i := 0; i < 20; i++ {
				if err := driver.Process(); !errors.Is(
					err, ErrStopped,
				) {
					t.Fatalf("incorrect stopped error: %v", err)
				}
			}

			if err := driver.SetReadStateHandler(nil); !errors.Is(
				err, ErrStopped,
			) {
				t.Fatal("stopped driver accepted handler change")
			}

			if err := driver.SetReadStateAppliedIndex(nil); !errors.Is(
				err, ErrStopped,
			) {
				t.Fatal("stopped driver accepted provider change")
			}

			if !reflect.DeepEqual(*events, previous) ||
				node.advances != advances ||
				len(node.acknowledged) != 0 {
				t.Fatal("stopped driver performed additional work")
			}
		})
	}
}

func TestDriverReadFailureNoMessagesNoTransport(t *testing.T) {
	ready := readStateTestBatch()
	ready.Messages = nil

	driver, node, _ := newReadStateTestDriver(
		t, ready, "send",
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

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if driver.Stopped() ||
		delivered != 1 ||
		len(node.acknowledged) != 1 {
		t.Fatal("message-free batch failed unnecessarily")
	}
}

func TestDriverReadFailureProviderNotInvoked(t *testing.T) {
	for _, stage := range []string{
		"append", "hard-state", "sync", "apply", "send",
	} {
		t.Run(stage, func(t *testing.T) {
			driver, _, _ := newReadStateTestDriver(
				t, readStateTestBatch(), stage,
			)

			if err := driver.SetReadStateAppliedIndex(
				func() uint64 {
					t.Fatal("failed batch evaluated read barrier")
					return 100
				},
			); err != nil {
				t.Fatal(err)
			}

			if err := driver.SetReadStateHandler(
				func(state raft.ReadState) error {
					t.Fatal("failed batch invoked consumer")
					return nil
				},
			); err != nil {
				t.Fatal(err)
			}

			if err := driver.Process(); err == nil {
				t.Fatal("expected injected failure")
			}
		})
	}
}

func TestDriverReadFailureHandlerAfterSuccessfulSend(t *testing.T) {
	driver, node, events := newReadStateTestDriver(
		t, readStateTestBatch(), "",
	)

	injected := errors.New("consumer rejected confirmation")

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			*events = append(*events, "deliver-read")
			return injected
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); !errors.Is(err, injected) {
		t.Fatalf("incorrect consumer error: %v", err)
	}

	expected := []string{
		"append",
		"hard-state",
		"sync",
		"apply",
		"advance",
		"send",
		"deliver-read",
	}

	if !reflect.DeepEqual(*events, expected) {
		t.Fatalf("incorrect failure order: %v", *events)
	}

	if !driver.Stopped() ||
		node.advances != 1 ||
		len(node.acknowledged) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("failed consumer acknowledgment")
	}
}
