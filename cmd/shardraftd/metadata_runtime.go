package main

import (
	"errors"
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

type metadataRuntime struct {
	group         *meta.Group
	sender        *transport.GRPCTransport
	bootstrapTerm uint64
}

func openMetadataRuntime(
	cfg config,
	remote []transport.Peer,
) (*metadataRuntime, error) {
	nodeID := raft.NodeID(cfg.id)

	if !meta.IsMetadataReplica(nodeID) {
		return nil, nil
	}

	addresses := make(map[raft.NodeID]string)

	for _, peer := range remote {
		addresses[peer.ID] = peer.Address
	}

	var peers []transport.Peer

	for _, id := range meta.MetadataReplicas() {
		if id == nodeID {
			continue
		}

		address := addresses[id]

		if address == "" {
			return nil, fmt.Errorf(
				"missing metadata peer address: %d",
				id,
			)
		}

		peers = append(peers, transport.Peer{
			ID:      id,
			Address: address,
		})
	}

	sender, err := transport.NewGRPCTransport(
		uint64(meta.MetadataGroupID),
		peers,
		transportQueueSize,
	)

	if err != nil {
		return nil, err
	}

	group, err := meta.OpenGroup(meta.GroupConfig{
		ID:              nodeID,
		DataDir:         cfg.dataDir,
		ElectionTimeout: cfg.electionTimeout,
		Send:            sender.Send,
	})

	if err != nil {
		return nil, errors.Join(
			err,
			sender.Close(),
		)
	}

	return &metadataRuntime{
		group:  group,
		sender: sender,
	}, nil
}

func (m *metadataRuntime) Tick() error {
	if m == nil {
		return nil
	}

	if err := m.group.Tick(); err != nil {
		return err
	}

	state, err := m.group.Status()
	if err != nil {
		return err
	}

	if state.Role != raft.Leader {
		return nil
	}

	version, err := m.group.Version()
	if err != nil {
		return err
	}

	if version != 0 ||
		m.bootstrapTerm == state.Term {
		return nil
	}

	if err := m.group.Bootstrap(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil
		}

		return err
	}

	m.bootstrapTerm = state.Term

	return nil
}

func (m *metadataRuntime) Group() *meta.Group {
	if m == nil {
		return nil
	}

	return m.group
}

func (m *metadataRuntime) Close() error {
	if m == nil {
		return nil
	}

	return errors.Join(
		m.group.Close(),
		m.sender.Close(),
	)
}
