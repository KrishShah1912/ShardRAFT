package observability

import (
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestPublishAssignsMonotonicEventIDs(t *testing.T) {
	bus := NewBus(10)

	first := bus.Publish(Event{
		NodeID:   raft.NodeID(1),
		Category: CategoryRaft,
		Type:     EventRaftElectionStarted,
	})

	second := bus.Publish(Event{
		NodeID:   raft.NodeID(1),
		Category: CategoryRaft,
		Type:     EventRaftLeaderElected,
	})

	if first.ID != 1 {
		t.Fatalf("expected first event ID 1, got %d", first.ID)
	}

	if second.ID != 2 {
		t.Fatalf("expected second event ID 2, got %d", second.ID)
	}

	if second.ID <= first.ID {
		t.Fatalf(
			"event IDs must increase: first=%d second=%d",
			first.ID,
			second.ID,
		)
	}
}

func TestPublishAssignsTimestamp(t *testing.T) {
	bus := NewBus(10)

	event := bus.Publish(Event{
		NodeID:   raft.NodeID(1),
		Category: CategoryNode,
		Type:     EventNodeStarted,
	})

	if event.Timestamp.IsZero() {
		t.Fatal("expected timestamp")
	}

	if event.Timestamp.Location() != time.UTC {
		t.Fatalf(
			"expected UTC timestamp, got %v",
			event.Timestamp.Location(),
		)
	}
}

func TestSubscriberReceivesPublishedEvent(t *testing.T) {
	bus := NewBus(10)

	sub := bus.Subscribe(4)
	defer bus.Unsubscribe(sub.ID)

	published := bus.Publish(Event{
		NodeID:   raft.NodeID(2),
		Category: CategoryRaft,
		Type:     EventRaftVoteGranted,
		Term:     7,
	})

	select {
	case received := <-sub.Events:
		if received.ID != published.ID {
			t.Fatalf(
				"expected event ID %d, got %d",
				published.ID,
				received.ID,
			)
		}

		if received.Type != EventRaftVoteGranted {
			t.Fatalf(
				"expected type %q, got %q",
				EventRaftVoteGranted,
				received.Type,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestMultipleSubscribersReceiveEvent(t *testing.T) {
	bus := NewBus(10)

	first := bus.Subscribe(2)
	second := bus.Subscribe(2)

	defer bus.Unsubscribe(first.ID)
	defer bus.Unsubscribe(second.ID)

	bus.Publish(Event{
		Category: CategoryNode,
		Type:     EventNodeStarted,
	})

	for _, sub := range []Subscriber{first, second} {
		select {
		case <-sub.Events:
		case <-time.After(time.Second):
			t.Fatal("subscriber did not receive event")
		}
	}
}

func TestSlowSubscriberDoesNotBlockPublish(t *testing.T) {
	bus := NewBus(10)

	sub := bus.Subscribe(1)
	defer bus.Unsubscribe(sub.ID)

	bus.Publish(Event{
		Type: EventNodeStarted,
	})

	done := make(chan struct{})

	go func() {
		for i := 0; i < 1000; i++ {
			bus.Publish(Event{
				Type: EventRaftHeartbeatSent,
			})
		}

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("slow subscriber blocked event publication")
	}
}

func TestHistoryIsBounded(t *testing.T) {
	bus := NewBus(3)

	for i := 0; i < 5; i++ {
		bus.Publish(Event{
			Type: EventRaftHeartbeatSent,
		})
	}

	history := bus.History()

	if len(history) != 3 {
		t.Fatalf(
			"expected history length 3, got %d",
			len(history),
		)
	}

	if history[0].ID != 3 ||
		history[1].ID != 4 ||
		history[2].ID != 5 {
		t.Fatalf(
			"unexpected history IDs: %d %d %d",
			history[0].ID,
			history[1].ID,
			history[2].ID,
		)
	}
}

func TestEventsAfter(t *testing.T) {
	bus := NewBus(10)

	for i := 0; i < 5; i++ {
		bus.Publish(Event{
			Type: EventRaftHeartbeatSent,
		})
	}

	events := bus.EventsAfter(3)

	if len(events) != 2 {
		t.Fatalf(
			"expected 2 events after ID 3, got %d",
			len(events),
		)
	}

	if events[0].ID != 4 || events[1].ID != 5 {
		t.Fatalf(
			"unexpected event IDs: %d %d",
			events[0].ID,
			events[1].ID,
		)
	}
}

func TestHistoryDoesNotExposeInternalDataMap(t *testing.T) {
	bus := NewBus(10)

	bus.Publish(Event{
		Type: EventRaftCommitAdvanced,
		Data: map[string]any{
			"commit_index": uint64(10),
		},
	})

	history := bus.History()

	history[0].Data["commit_index"] = uint64(999)

	secondRead := bus.History()

	got := secondRead[0].Data["commit_index"]

	if got != uint64(10) {
		t.Fatalf(
			"history mutation leaked into bus: got %v",
			got,
		)
	}
}

func TestUnsubscribeRemovesSubscriber(t *testing.T) {
	bus := NewBus(10)

	sub := bus.Subscribe(1)

	if bus.SubscriberCount() != 1 {
		t.Fatalf(
			"expected 1 subscriber, got %d",
			bus.SubscriberCount(),
		)
	}

	bus.Unsubscribe(sub.ID)

	if bus.SubscriberCount() != 0 {
		t.Fatalf(
			"expected 0 subscribers, got %d",
			bus.SubscriberCount(),
		)
	}

	_, ok := <-sub.Events
	if ok {
		t.Fatal("expected subscriber channel to be closed")
	}
}

func TestLatestEventID(t *testing.T) {
	bus := NewBus(10)

	if bus.LatestEventID() != 0 {
		t.Fatal("new bus should start at event ID 0")
	}

	bus.Publish(Event{Type: EventNodeStarted})
	bus.Publish(Event{Type: EventNodeStopped})

	if bus.LatestEventID() != 2 {
		t.Fatalf(
			"expected latest ID 2, got %d",
			bus.LatestEventID(),
		)
	}
}
