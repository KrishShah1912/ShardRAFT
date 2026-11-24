package transport

import (
	"context"
	"errors"
)

// GRPCTransport uses the existing queued transport
// infrastructure with gRPC-based message delivery.
//
// The alias preserves compatibility with existing
// daemon, administration and observability APIs.
type GRPCTransport = HTTPTransport

// NewGRPCTransport creates a queued Raft transport
// using reusable gRPC connections.
//
// Each peer retains its independent queue, retry
// worker, statistics and backpressure handling.
func NewGRPCTransport(
	shardID uint64,
	peers []Peer,
	queueSize int,
) (*GRPCTransport, error) {
	if queueSize <= 0 {
		return nil, errors.New(
			"queue size must be positive",
		)
	}

	delivery, err := NewGRPCDelivery(
		shardID,
		peers,
	)
	if err != nil {
		return nil, err
	}

	transport, err := newQueuedTransport(
		peers,
		queueSize,
		func(
			ctx context.Context,
			item outgoing,
		) error {
			return delivery.Deliver(
				ctx,
				item.message,
			)
		},
		delivery.Close,
	)

	if err != nil {
		_ = delivery.Close()
		return nil, err
	}

	return transport, nil
}
