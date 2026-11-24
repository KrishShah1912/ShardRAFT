package main

import (
	"time"

	"github.com/KrishShah1912/shardraft/internal/observability"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

func connectTransportObservability(
	sender *transport.HTTPTransport,
	bus *observability.Bus,
	nodeID raft.NodeID,
) {
	if sender == nil || bus == nil {
		return
	}

	sender.SetEventHandler(
		func(event transport.Event) {
			bus.Publish(
				observability.Event{
					Timestamp: time.Now().UTC(),
					NodeID:    nodeID,
					Category:  observability.CategoryTransport,
					Type:      event.Type,
					Data:      event.Data,
				},
			)
		},
	)
}
