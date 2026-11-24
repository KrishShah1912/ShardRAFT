package raftdriver

import (
	"errors"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type readStateTestNode struct {
	ready raft.Ready

	events *[]string

	acknowledged []raft.ReadState

	advances int
}

func (n *readStateTestNode) Ready() raft.Ready {
	return n.ready
}

func (n *readStateTestNode) Advance() {
	n.advances++

	*n.events = append(
		*n.events,
		"advance",
	)

	n.ready.Entries = nil
	n.ready.CommittedEntries = nil
	n.ready.HardState = nil
	n.ready.Messages = nil
	n.ready.TruncateFrom = 0
}

func (n *readStateTestNode) AdvanceReadStates(
	states []raft.ReadState,
) {
	*n.events = append(
		*n.events,
		"acknowledge-read",
	)

	n.acknowledged = append(
		n.acknowledged,
		states...,
	)

	remaining := make(
		[]raft.ReadState,
		0,
		len(n.ready.ReadStates),
	)

	for _, current := range n.ready.ReadStates {
		remove := false

		for _, state := range states {
			if current.RequestID == state.RequestID &&
				current.Term == state.Term {
				remove = true
				break
			}
		}

		if !remove {
			remaining = append(
				remaining,
				current,
			)
		}
	}

	n.ready.ReadStates = remaining
}

type readStateTestStorage struct {
	events *[]string

	failAt string
}

func (s *readStateTestStorage) record(
	event string,
) error {
	*s.events = append(*s.events, event)

	if s.failAt == event {
		return errors.New(
			"injected " + event + " failure",
		)
	}

	return nil
}

func (s *readStateTestStorage) AppendEntries(
	entries []raft.Entry,
) error {
	return s.record("append")
}

func (s *readStateTestStorage) ReplaceEntries(
	fromIndex uint64,
	entries []raft.Entry,
) error {
	return s.record("replace")
}

func (s *readStateTestStorage) TruncateSuffix(
	fromIndex uint64,
) error {
	return s.record("truncate")
}

func (s *readStateTestStorage) SaveHardState(
	state raft.HardState,
) error {
	return s.record("hard-state")
}

func (s *readStateTestStorage) Sync() error {
	return s.record("sync")
}

func readStateTestBatch() raft.Ready {
	entry := raft.Entry{
		Index: 1,
		Term:  1,
		Type:  raft.EntryNormal,
		Data:  []byte("command"),
	}

	return raft.Ready{
		Entries: []raft.Entry{entry},

		CommittedEntries: []raft.Entry{entry},

		HardState: &raft.HardState{
			Term:   1,
			Commit: 1,
		},

		ReadStates: []raft.ReadState{
			{
				RequestID: 1,
				Term:      1,
				Index:     1,
				Context:   []byte("first"),
			},
		},

		Messages: []raft.Message{
			{
				Type: raft.MsgHeartbeat,
				From: 1,
				To:   2,
				Term: 1,
			},
		},
	}
}

func newReadStateTestDriver(
	t *testing.T,
	ready raft.Ready,
	failAt string,
) (*Driver, *readStateTestNode, *[]string) {
	t.Helper()

	events := &[]string{}

	node := &readStateTestNode{
		ready:  ready,
		events: events,
	}

	storage := &readStateTestStorage{
		events: events,
		failAt: failAt,
	}

	driver, err := New(
		node,
		storage,
		func(entry raft.Entry) error {
			return storage.record("apply")
		},
		func(messages []raft.Message) error {
			return storage.record("send")
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateAppliedIndex(
		func() uint64 { return 1 },
	); err != nil {
		t.Fatal(err)
	}

	return driver, node, events
}

func TestDriverReadStatesProcessingOrder(t *testing.T) {
	driver, node, events :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"",
		)

	err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			*events = append(
				*events,
				"deliver-read",
			)

			if state.RequestID != 1 ||
				state.Index != 1 ||
				string(state.Context) != "first" {
				t.Fatal("incorrect read confirmation")
			}

			return nil
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"append",
		"hard-state",
		"sync",
		"apply",
		"advance",
		"send",
		"deliver-read",
		"acknowledge-read",
	}

	if !reflect.DeepEqual(*events, expected) {
		t.Fatalf(
			"incorrect order: got %v want %v",
			*events, expected,
		)
	}

	if len(node.acknowledged) != 1 ||
		len(node.ready.ReadStates) != 0 {
		t.Fatal("read was not acknowledged")
	}
}

func TestDriverReadStatesReadOnlyBatch(t *testing.T) {
	ready := raft.Ready{
		ReadStates: readStateTestBatch().ReadStates,
	}

	driver, node, events :=
		newReadStateTestDriver(t, ready, "")

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			*events = append(
				*events,
				"deliver-read",
			)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"sync",
		"advance",
		"deliver-read",
		"acknowledge-read",
	}

	if !reflect.DeepEqual(*events, expected) {
		t.Fatalf("unexpected operations: %v", *events)
	}

	if len(node.acknowledged) != 1 {
		t.Fatal("missing acknowledgment")
	}
}

func TestDriverReadStatesNoHandlerPreservesQueue(t *testing.T) {
	driver, node, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"",
		)

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(node.ready.ReadStates) != 1 ||
		len(node.acknowledged) != 0 {
		t.Fatal("unhandled read was discarded")
	}

	if node.advances != 1 {
		t.Fatal("legacy Ready processing changed")
	}
}

func TestDriverReadStatesLateHandlerRegistration(t *testing.T) {
	driver, node, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"",
		)

	if err := driver.Process(); err != nil {
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

	if delivered != 1 ||
		len(node.ready.ReadStates) != 0 {
		t.Fatal("preserved read was not delivered")
	}
}

func TestDriverReadStatesDisableHandler(t *testing.T) {
	driver, node, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"",
		)

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("disabled handler invoked")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateHandler(nil); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(node.ready.ReadStates) != 1 {
		t.Fatal("disabled handler lost read")
	}
}

func TestDriverReadStatesMultipleConfirmations(t *testing.T) {
	ready := readStateTestBatch()

	for id := uint64(2); id <= 10; id++ {
		ready.ReadStates = append(
			ready.ReadStates,
			raft.ReadState{
				RequestID: id,
				Term:      1,
				Index:     1,
				Context:   []byte("additional"),
			},
		)
	}

	driver, node, _ :=
		newReadStateTestDriver(t, ready, "")

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

	if len(delivered) != 10 ||
		len(node.acknowledged) != 10 ||
		len(node.ready.ReadStates) != 0 {
		t.Fatal("incomplete batch delivery")
	}

	for i, id := range delivered {
		if id != uint64(i+1) {
			t.Fatal("incorrect delivery order")
		}
	}
}

func TestDriverReadStatesNoDuplicateDelivery(t *testing.T) {
	driver, _, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"",
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

	for i := 0; i < 20; i++ {
		if err := driver.Process(); err != nil {
			t.Fatal(err)
		}
	}

	if delivered != 1 {
		t.Fatalf("duplicate delivery: %d", delivered)
	}
}

func TestDriverReadStatesContextIsolation(t *testing.T) {
	driver, node, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"",
		)

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			state.Context[0] = 'X'
			state.RequestID = 99
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(node.acknowledged) != 1 {
		t.Fatal("missing acknowledgment")
	}

	state := node.acknowledged[0]

	if state.RequestID != 1 ||
		string(state.Context) != "first" {
		t.Fatal("callback modified acknowledgment")
	}
}

func TestDriverReadStatesPersistenceFailures(t *testing.T) {
	for _, stage := range []string{
		"append",
		"hard-state",
		"sync",
		"apply",
		"replace",
		"truncate",
	} {
		t.Run(stage, func(t *testing.T) {
			ready := readStateTestBatch()

			if stage == "replace" {
				ready.TruncateFrom = 1
			}

			if stage == "truncate" {
				ready.TruncateFrom = 1
				ready.Entries = nil
			}

			driver, node, _ :=
				newReadStateTestDriver(
					t, ready, stage,
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
				t.Fatal("expected injected failure")
			}

			if delivered != 0 ||
				len(node.acknowledged) != 0 ||
				node.advances != 0 {
				t.Fatal("failed batch delivered read")
			}

			if !driver.Stopped() {
				t.Fatal("driver did not stop")
			}

			if len(node.ready.ReadStates) != 1 {
				t.Fatal("failed batch lost confirmation")
			}
		})
	}
}

func TestDriverReadStatesHandlerFailure(t *testing.T) {
	driver, node, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"",
		)

	injected := errors.New("consumer failed")

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			return injected
		},
	); err != nil {
		t.Fatal(err)
	}

	err := driver.Process()

	if !errors.Is(err, injected) {
		t.Fatalf("incorrect error: %v", err)
	}

	if !driver.Stopped() ||
		node.advances != 1 ||
		len(node.acknowledged) != 0 ||
		len(node.ready.ReadStates) != 1 {
		t.Fatal("failed delivery was acknowledged")
	}
}

func TestDriverReadStatesPartialHandlerFailure(t *testing.T) {
	ready := readStateTestBatch()

	ready.ReadStates = append(
		ready.ReadStates,
		raft.ReadState{
			RequestID: 2,
			Term:      1,
			Index:     1,
			Context:   []byte("second"),
		},
		raft.ReadState{
			RequestID: 3,
			Term:      1,
			Index:     1,
			Context:   []byte("third"),
		},
	)

	driver, node, _ :=
		newReadStateTestDriver(t, ready, "")

	var delivered []uint64

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			delivered = append(
				delivered,
				state.RequestID,
			)

			if state.RequestID == 2 {
				return errors.New("consumer failure")
			}

			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err == nil {
		t.Fatal("expected partial failure")
	}

	if !reflect.DeepEqual(
		delivered,
		[]uint64{1, 2},
	) {
		t.Fatal("delivery continued after failure")
	}

	if len(node.acknowledged) != 1 ||
		node.acknowledged[0].RequestID != 1 {
		t.Fatal("incorrect partial acknowledgment")
	}

	if len(node.ready.ReadStates) != 2 ||
		node.ready.ReadStates[0].RequestID != 2 ||
		node.ready.ReadStates[1].RequestID != 3 {
		t.Fatal("undelivered confirmations were lost")
	}
}

func TestDriverReadStatesStoppedDriver(t *testing.T) {
	driver, node, _ :=
		newReadStateTestDriver(
			t,
			readStateTestBatch(),
			"sync",
		)

	if err := driver.Process(); err == nil {
		t.Fatal("expected driver failure")
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("stopped driver delivered read")
			return nil
		},
	); !errors.Is(err, ErrStopped) {
		t.Fatalf("incorrect error: %v", err)
	}

	if err := driver.Process(); !errors.Is(err, ErrStopped) {
		t.Fatalf("incorrect stopped result: %v", err)
	}

	if len(node.acknowledged) != 0 {
		t.Fatal("stopped driver acknowledged read")
	}
}

func TestDriverReadStatesLegacyConsensus(t *testing.T) {
	events := []string{}

	node := &fakeNode{}

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

	err = driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			return nil
		},
	)

	if !errors.Is(err, ErrReadStateUnsupported) {
		t.Fatalf("incorrect compatibility error: %v", err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if !node.advanced {
		t.Fatal("legacy driver did not advance")
	}
}

func TestDriverReadStatesEmptyBatch(t *testing.T) {
	driver, node, _ :=
		newReadStateTestDriver(
			t,
			raft.Ready{},
			"",
		)

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			t.Fatal("empty batch invoked handler")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if len(node.acknowledged) != 0 {
		t.Fatal("empty batch acknowledged read")
	}
}

func TestDriverReadStatesActualRaftNode(t *testing.T) {
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
		func() uint64 { return applied },
	); err != nil {
		t.Fatal(err)
	}

	delivered := 0

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			delivered++

			if state.Index > applied {
				t.Fatal("read delivered before application")
			}

			if string(state.Context) != "actual-node" {
				t.Fatal("incorrect read context")
			}

			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := node.Propose(
		[]byte("command"),
	); err != nil {
		t.Fatal(err)
	}

	if err := node.ReadIndex(
		[]byte("actual-node"),
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if delivered != 1 ||
		len(node.Ready().ReadStates) != 0 {
		t.Fatal("actual Raft confirmation not consumed")
	}

	if err := driver.Process(); err != nil {
		t.Fatal(err)
	}

	if delivered != 1 {
		t.Fatal("actual Raft read delivered twice")
	}
}
