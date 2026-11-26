package main

import (
	"github.com/KrishShah1912/shardraft/internal/observability"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

func connectMultiTransportEvents(
	sender *multiRaftTransport,
	bus *observability.Bus,
	nodeID raft.NodeID,
) {
	if sender == nil || bus == nil {
		return
	}

	sender.mu.RLock()
	defer sender.mu.RUnlock()

	if sender.closed {
		return
	}

	for id, group := range sender.groups {
		shardID := uint64(id)

		if group == nil {
			continue
		}

		group.SetEventHandler(
			func(event transport.Event) {
				bus.Publish(
					observability.Event{
						NodeID:   nodeID,
						ShardID:  shardID,
						Category: observability.CategoryTransport,
						Type:     event.Type,
						Data:     event.Data,
					},
				)
			},
		)
	}
}
