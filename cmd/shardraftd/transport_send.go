package main

import (
	"errors"
	"log"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

// sendRaftMessages reports temporary transport congestion without
// stopping the Raft driver. Other transport errors remain fatal.
func sendRaftMessages(
	sender *transport.HTTPTransport,
	messages []raft.Message,
) error {
	if sender == nil {
		return nil
	}

	err := sender.Send(messages)
	if errors.Is(err, transport.ErrQueueFull) {
		log.Printf("Raft transport backpressure: %v", err)
		return nil
	}

	return err
}
