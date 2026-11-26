package observability

import (
	"encoding/json"
	"testing"
)

func TestShardEventIdentity(t *testing.T) {
	bus := NewBus(10)

	bus.Publish(Event{
		NodeID:   1,
		ShardID:  1,
		Category: CategoryRaft,
		Type:     EventRaftLeaderElected,
	})

	bus.Publish(Event{
		NodeID:   1,
		ShardID:  4,
		Category: CategoryTransport,
		Type:     EventTransportDelivered,
	})

	history := bus.History()

	if len(history) != 2 {
		t.Fatalf("expected two events, got %d", len(history))
	}

	if history[0].ShardID != 1 || history[1].ShardID != 4 {
		t.Fatal("shard identity was not preserved")
	}

	if history[0].ID >= history[1].ID {
		t.Fatal("event IDs are not increasing")
	}

	payload, err := json.Marshal(history[1])

	if err != nil {
		t.Fatal(err)
	}

	var decoded Event

	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.ShardID != 4 {
		t.Fatal("shard identity lost during JSON serialization")
	}
}
