package raft

import "testing"

func containsEvent(events []Event, eventType string) bool {
	for _, event := range events {
		if event.Type == eventType {
			return true
		}
	}

	return false
}

func TestRaftEmitsElectionEvents(t *testing.T) {
	r := NewRaft(1, []NodeID{2, 3}, 1)

	var events []Event

	r.SetEventHandler(func(event Event) {
		events = append(events, event)
	})

	r.Tick()

	if !containsEvent(events, EventElectionStarted) {
		t.Fatal("expected election started event")
	}

	if !containsEvent(events, EventTermChanged) {
		t.Fatal("expected term changed event")
	}

	if !containsEvent(events, EventRoleChanged) {
		t.Fatal("expected role changed event")
	}

	if !containsEvent(events, EventVoteRequested) {
		t.Fatal("expected vote requested event")
	}
}

func TestRaftEmitsLeaderEvent(t *testing.T) {
	r := NewRaft(1, nil, 1)

	var events []Event

	r.SetEventHandler(func(event Event) {
		events = append(events, event)
	})

	r.Tick()

	if !containsEvent(events, EventLeaderElected) {
		t.Fatal("expected leader elected event")
	}

	if !containsEvent(events, EventNoopAppended) {
		t.Fatal("expected leader no-op event")
	}
}

func TestLeaderEmitsEntryAppendedEvent(t *testing.T) {
	r := NewRaft(1, nil, 1)

	var events []Event

	r.SetEventHandler(func(event Event) {
		events = append(events, event)
	})

	r.Tick()

	events = nil

	if err := r.Propose([]byte("SET x=10")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	if !containsEvent(events, EventEntryAppended) {
		t.Fatal("expected entry appended event")
	}
}

func TestLeaderEmitsCommitAdvancedEvent(t *testing.T) {
	r := NewRaft(1, nil, 1)

	var events []Event

	r.SetEventHandler(func(event Event) {
		events = append(events, event)
	})

	r.Tick()

	if !containsEvent(events, EventCommitAdvanced) {
		t.Fatal("expected commit advanced event")
	}
}

func TestAppendEntriesEmitsReceivedAndAccepted(t *testing.T) {
	r := NewRaft(2, []NodeID{1}, 10)

	var events []Event

	r.SetEventHandler(func(event Event) {
		events = append(events, event)
	})

	err := r.Step(Message{
		Type:      MsgAppend,
		From:      1,
		To:        2,
		Term:      1,
		Index:     0,
		LogTerm:   0,
		Commit:    0,
		RequestID: 42,
		Entries: []Entry{
			{
				Term:  1,
				Index: 1,
				Type:  EntryNormal,
				Data:  []byte("test"),
			},
		},
	})
	if err != nil {
		t.Fatalf("step: %v", err)
	}

	if !containsEvent(events, EventAppendReceived) {
		t.Fatal("expected append received event")
	}

	if !containsEvent(events, EventAppendAccepted) {
		t.Fatal("expected append accepted event")
	}
}

func TestAppendEntriesEmitsRejected(t *testing.T) {
	r := NewRaft(2, []NodeID{1}, 10)

	var events []Event

	r.SetEventHandler(func(event Event) {
		events = append(events, event)
	})

	err := r.Step(Message{
		Type:      MsgAppend,
		From:      1,
		To:        2,
		Term:      1,
		Index:     5,
		LogTerm:   1,
		RequestID: 43,
	})
	if err != nil {
		t.Fatalf("step: %v", err)
	}

	if !containsEvent(events, EventAppendReceived) {
		t.Fatal("expected append received event")
	}

	if !containsEvent(events, EventAppendRejected) {
		t.Fatal("expected append rejected event")
	}
}

func TestEventHandlerDoesNotChangeRaftBehavior(t *testing.T) {
	withoutEvents := NewRaft(1, nil, 1)
	withEvents := NewRaft(1, nil, 1)

	withEvents.SetEventHandler(func(Event) {})

	withoutEvents.Tick()
	withEvents.Tick()

	a := withoutEvents.Status()
	b := withEvents.Status()

	if a != b {
		t.Fatalf(
			"event handler changed Raft state: without=%+v with=%+v",
			a,
			b,
		)
	}
}

func TestNilEventHandlerIsSafe(t *testing.T) {
	r := NewRaft(1, nil, 1)

	r.Tick()

	if r.Status().Role != Leader {
		t.Fatal("expected single node to become leader")
	}
}
