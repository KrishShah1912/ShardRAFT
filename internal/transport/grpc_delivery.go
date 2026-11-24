package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const grpcDeliveryTimeout = 2 * time.Second

// GRPCDelivery manages reusable connections for
// sending Raft messages to configured peers.
//
// Insecure credentials are intended for trusted
// local development environments only.
type GRPCDelivery struct {
	mu sync.RWMutex

	shardID uint64
	closed  bool

	connections map[raft.NodeID]*grpc.ClientConn
	clients     map[raft.NodeID]shardraftpb.RaftServiceClient
}

// NewGRPCDelivery creates one reusable gRPC
// connection per configured peer.
func NewGRPCDelivery(
	shardID uint64,
	peers []Peer,
) (*GRPCDelivery, error) {
	if shardID == 0 {
		return nil, errors.New("shard ID must be nonzero")
	}

	d := &GRPCDelivery{
		shardID: shardID,
		connections: make(
			map[raft.NodeID]*grpc.ClientConn,
		),
		clients: make(
			map[raft.NodeID]shardraftpb.RaftServiceClient,
		),
	}

	for _, peer := range peers {
		if peer.ID == 0 || peer.Address == "" {
			_ = d.Close()
			return nil, errors.New("invalid gRPC peer")
		}

		if _, exists := d.connections[peer.ID]; exists {
			_ = d.Close()
			return nil, fmt.Errorf(
				"duplicate gRPC peer ID %d",
				peer.ID,
			)
		}

		conn, err := grpc.NewClient(
			peer.Address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
			grpc.WithDisableRetry(),
		)
		if err != nil {
			_ = d.Close()
			return nil, fmt.Errorf(
				"create gRPC connection for peer %d: %w",
				peer.ID,
				err,
			)
		}

		d.connections[peer.ID] = conn
		d.clients[peer.ID] =
			shardraftpb.NewRaftServiceClient(conn)
	}

	return d, nil
}

// Deliver performs one bounded gRPC delivery attempt.
// Queueing and application-level retries are handled
// separately by the transport worker.
func (d *GRPCDelivery) Deliver(
	ctx context.Context,
	message raft.Message,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.RLock()

	if d.closed {
		d.mu.RUnlock()
		return ErrClosed
	}

	client, exists := d.clients[message.To]
	shardID := d.shardID

	d.mu.RUnlock()

	if !exists {
		return fmt.Errorf(
			"%w: %d",
			ErrPeer,
			message.To,
		)
	}

	if message.From == 0 ||
		message.To == 0 ||
		message.From == message.To {
		return errors.New("invalid Raft message endpoints")
	}

	encoded, err := EncodeRaftMessage(message)
	if err != nil {
		return err
	}

	rpcCtx, cancel := context.WithTimeout(
		ctx,
		grpcDeliveryTimeout,
	)
	defer cancel()

	response, err := client.Step(
		rpcCtx,
		&shardraftpb.StepRequest{
			ShardId: shardID,
			Message: encoded,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"gRPC delivery to peer %d: %w",
			message.To,
			err,
		)
	}

	if response == nil || !response.Accepted {
		return fmt.Errorf(
			"peer %d did not accept Raft message",
			message.To,
		)
	}

	return nil
}

// Close releases all peer connections.
// Repeated calls are safe.
func (d *GRPCDelivery) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true

	var closeErrors []error

	for id, conn := range d.connections {
		if err := conn.Close(); err != nil {
			closeErrors = append(
				closeErrors,
				fmt.Errorf(
					"close peer %d: %w",
					id,
					err,
				),
			)
		}
	}

	return errors.Join(closeErrors...)
}
