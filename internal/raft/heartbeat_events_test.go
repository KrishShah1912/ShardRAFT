package raft

import "testing"

func hasEventType(events []Event, eventType string) bool {
	for _, event := range events {
		if event.Type == eventType {
			return true
		}
	}

	return false
}

func TestFollowerClassifiesEmptyAppendAsHeartbeat(t *testing.T) {
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
		RequestID: 99,
		Entries:   nil,
	})
	if err != nil {
		t.Fatalf("step heartbeat: %v", err)
	}

	if !hasEventType(events, EventHeartbeatReceived) {
		t.Fatal("expected heartbeat received event")
	}

	if !hasEventType(events, EventHeartbeatAccepted) {
		t.Fatal("expected heartbeat accepted event")
	}

	if hasEventType(events, EventAppendReceived) {
		t.Fatal(
			"empty AppendEntries must not emit append received",
		)
	}

	if hasEventType(events, EventAppendAccepted) {
		t.Fatal(
			"empty AppendEntries must not emit append accepted",
		)
	}
}

func TestFollowerClassifiesEntriesAsAppendEvents(t *testing.T) {
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
		RequestID: 100,
		Entries: []Entry{
			{
				Index: 1,
				Term:  1,
				Type:  EntryNormal,
				Data:  []byte("test"),
			},
		},
	})
	if err != nil {
		t.Fatalf("step append: %v", err)
	}

	if !hasEventType(events, EventAppendReceived) {
		t.Fatal("expected append received event")
	}

	if !hasEventType(events, EventAppendAccepted) {
		t.Fatal("expected append accepted event")
	}

	if hasEventType(events, EventHeartbeatReceived) {
		t.Fatal(
			"non-empty AppendEntries must not emit heartbeat received",
		)
	}

	if hasEventType(events, EventHeartbeatAccepted) {
		t.Fatal(
			"non-empty AppendEntries must not emit heartbeat accepted",
		)
	}
}
