package main

import (
	"errors"
	"fmt"
	"sync"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

type multiRaftTransport struct {
	mu     sync.RWMutex
	nodeID raft.NodeID
	groups map[meta.ShardID]*transport.GRPCTransport
	closed bool
}

func newMultiRaftTransport(
	nodeID raft.NodeID,
	layout meta.ShardMap,
	peers []transport.Peer,
	queueSize int,
) (*multiRaftTransport, error) {
	if nodeID == 0 {
		return nil, errors.New("node ID is required")
	}

	if queueSize <= 0 {
		return nil, errors.New("queue size must be positive")
	}

	router, err := shard.NewRouter(layout)
	if err != nil {
		return nil, err
	}

	addresses := make(map[raft.NodeID]string)

	for _, peer := range peers {
		if peer.ID == 0 ||
			peer.ID == nodeID ||
			peer.Address == "" {
			return nil, errors.New("invalid remote peer")
		}

		if _, exists := addresses[peer.ID]; exists {
			return nil, fmt.Errorf(
				"duplicate peer %d", peer.ID,
			)
		}

		addresses[peer.ID] = peer.Address
	}

	result := &multiRaftTransport{
		nodeID: nodeID,
		groups: make(
			map[meta.ShardID]*transport.GRPCTransport,
		),
	}

	for _, placement := range router.ShardMap().Shards {
		local := false

		for _, replica := range placement.Replicas {
			if replica == nodeID {
				local = true
				break
			}
		}

		if !local {
			continue
		}

		var groupPeers []transport.Peer

		for _, replica := range placement.Replicas {
			if replica == nodeID {
				continue
			}

			address, exists := addresses[replica]
			if !exists {
				return nil, errors.Join(
					fmt.Errorf(
						"missing address for shard %d peer %d",
						placement.ID,
						replica,
					),
					result.Close(),
				)
			}

			groupPeers = append(
				groupPeers,
				transport.Peer{
					ID:      replica,
					Address: address,
				},
			)
		}

		sender, err := transport.NewGRPCTransport(
			uint64(placement.ID),
			groupPeers,
			queueSize,
		)

		if err != nil {
			return nil, errors.Join(
				err,
				result.Close(),
			)
		}

		result.groups[placement.ID] = sender
	}

	if len(result.groups) == 0 {
		return nil, shard.ErrGroupNotLocal
	}

	return result, nil
}

func (t *multiRaftTransport) Send(
	groupID meta.ShardID,
	messages []raft.Message,
) error {
	t.mu.RLock()

	if t.closed {
		t.mu.RUnlock()
		return transport.ErrClosed
	}

	sender, exists := t.groups[groupID]

	t.mu.RUnlock()

	if !exists {
		return fmt.Errorf(
			"%w: %d",
			shard.ErrGroupNotLocal,
			groupID,
		)
	}

	for _, message := range messages {
		if message.From != t.nodeID ||
			message.To == t.nodeID {
			return errors.New(
				"invalid local Raft message endpoints",
			)
		}
	}

	return sender.Send(messages)
}

func (t *multiRaftTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true

	var result error

	for id, sender := range t.groups {
		if err := sender.Close(); err != nil {
			result = errors.Join(
				result,
				fmt.Errorf(
					"close shard %d transport: %w",
					id,
					err,
				),
			)
		}
	}

	return result
}

var _ shard.GroupSendFunc = (*multiRaftTransport)(nil).Send
