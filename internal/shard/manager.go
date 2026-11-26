package shard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

var ErrGroupNotLocal = errors.New(
	"raft group is not hosted locally",
)

type GroupSendFunc func(
	groupID meta.ShardID,
	messages []raft.Message,
) error

type ManagerConfig struct {
	NodeID          raft.NodeID
	DataDir         string
	ElectionTimeout int
	ShardMap        meta.ShardMap
	SegmentOptions  wal.SegmentOptions
	Send            GroupSendFunc
}

type Manager struct {
	mu     sync.RWMutex
	groups map[meta.ShardID]*Group
	router *Router
	closed bool
}

func OpenManager(cfg ManagerConfig) (*Manager, error) {
	if cfg.NodeID == 0 {
		return nil, errors.New("node ID is required")
	}

	if cfg.DataDir == "" {
		return nil, errors.New("data directory is required")
	}

	if cfg.ElectionTimeout <= 0 {
		return nil, errors.New(
			"election timeout must be positive",
		)
	}

	if cfg.Send == nil {
		return nil, errors.New(
			"group send callback is required",
		)
	}

	router, err := NewRouter(cfg.ShardMap)
	if err != nil {
		return nil, err
	}

	manager := &Manager{
		groups: make(map[meta.ShardID]*Group),
		router: router,
	}

	for _, placement := range router.ShardMap().Shards {
		local := false
		var peers []raft.NodeID

		for _, replica := range placement.Replicas {
			if replica == cfg.NodeID {
				local = true
			} else {
				peers = append(peers, replica)
			}
		}

		if !local {
			continue
		}

		groupID := placement.ID

		dir := filepath.Join(
			cfg.DataDir,
			fmt.Sprintf("shard-%d", groupID),
		)

		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, errors.Join(
				err,
				manager.Close(),
			)
		}

		instance, err := Open(Config{
			ID:              cfg.NodeID,
			Peers:           peers,
			ElectionTimeout: cfg.ElectionTimeout,
			WALPath: filepath.Join(
				dir, "raft.wal",
			),
			SnapshotPath: filepath.Join(
				dir, "raft.snapshot",
			),
			SegmentOptions: cfg.SegmentOptions,
			Send: func(messages []raft.Message) error {
				return cfg.Send(groupID, messages)
			},
		})

		if err != nil {
			return nil, errors.Join(
				fmt.Errorf(
					"open shard %d: %w",
					groupID,
					err,
				),
				manager.Close(),
			)
		}

		manager.groups[groupID] = NewGroup(
			groupID,
			instance,
			router,
		)
	}

	if len(manager.groups) == 0 {
		return nil, ErrGroupNotLocal
	}

	return manager, nil
}

func (m *Manager) GroupIDs() []meta.ShardID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]meta.ShardID, 0, len(m.groups))

	for id := range m.groups {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	return ids
}

func (m *Manager) Group(
	id meta.ShardID,
) (*Group, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrClosed
	}

	group, ok := m.groups[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: %d",
			ErrGroupNotLocal,
			id,
		)
	}

	return group, nil
}

func (m *Manager) GroupForKey(
	key string,
) (*Group, error) {
	route, err := m.router.Route(key)
	if err != nil {
		return nil, err
	}

	group, err := m.Group(route.GroupID)

	if errors.Is(err, ErrGroupNotLocal) {
		return nil, &NotLocalError{
			Key:      key,
			Slot:     route.Slot,
			Owner:    route.GroupID,
			Replicas: route.Replicas,
		}
	}

	return group, err
}

func (m *Manager) ShardMap() meta.ShardMap {
	return m.router.ShardMap()
}

func (m *Manager) Tick(
	id meta.ShardID,
) error {
	group, err := m.Group(id)
	if err != nil {
		return err
	}

	return group.Shard().Tick()
}

func (m *Manager) Step(
	id meta.ShardID,
	message raft.Message,
) error {
	group, err := m.Group(id)
	if err != nil {
		return err
	}

	return group.Shard().Step(message)
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true

	var result error

	for id, group := range m.groups {
		if err := group.Shard().Close(); err != nil {
			result = errors.Join(
				result,
				fmt.Errorf(
					"close shard %d: %w",
					id,
					err,
				),
			)
		}
	}

	clear(m.groups)

	return result
}
