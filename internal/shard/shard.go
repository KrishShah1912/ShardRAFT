package shard

import (
	"errors"
	"fmt"
	"sync"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/raftdriver"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

var ErrClosed = errors.New("shard is closed")

type Config struct {
	ID              raft.NodeID
	Peers           []raft.NodeID
	ElectionTimeout int
	WALPath         string
	SnapshotPath    string
	SegmentOptions  wal.SegmentOptions
	Send            raftdriver.SendFunc
}

type Shard struct {
	mu sync.Mutex

	node         *raft.Raft
	storage      *wal.SegmentedWAL
	driver       *raftdriver.Driver
	store        *kvstore.Store
	snapshotPath string

	closed  bool
	stopped bool
}

func Open(cfg Config) (*Shard, error) {
	if cfg.ID == 0 {
		return nil, errors.New("node ID must be nonzero")
	}
	if cfg.WALPath == "" {
		return nil, errors.New("WAL path is required")
	}
	if cfg.ElectionTimeout <= 0 {
		return nil, errors.New("election timeout must be positive")
	}
	if cfg.Send == nil {
		return nil, errors.New("send callback is required")
	}

	seen := map[raft.NodeID]bool{cfg.ID: true}
	for _, peer := range cfg.Peers {
		if peer == 0 || seen[peer] {
			return nil, fmt.Errorf(
				"invalid or duplicate peer ID: %d", peer,
			)
		}
		seen[peer] = true
	}

	snapshotPath := cfg.SnapshotPath
	if snapshotPath == "" {
		snapshotPath = cfg.WALPath + ".snapshot"
	}

	node, storage, driver, store, err :=
		raftdriver.RecoverSegmentedKVDriver(
			cfg.WALPath,
			snapshotPath,
			cfg.SegmentOptions,
			cfg.ID,
			cfg.Peers,
			cfg.ElectionTimeout,
			cfg.Send,
		)
	if err != nil {
		return nil, err
	}

	if err := driver.Process(); err != nil {
		_ = storage.Close()
		return nil, fmt.Errorf(
			"replay committed entries: %w", err,
		)
	}

	return &Shard{
		node:         node,
		storage:      storage,
		driver:       driver,
		store:        store,
		snapshotPath: snapshotPath,
	}, nil
}

func (s *Shard) Tick() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	if s.stopped || s.driver.Stopped() {
		return ErrStopped
	}

	s.node.Tick()
	return s.driver.Process()
}

func (s *Shard) Step(msg raft.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	if s.stopped || s.driver.Stopped() {
		return ErrStopped
	}

	if err := s.node.Step(msg); err != nil {
		return err
	}

	return s.driver.Process()
}

func (s *Shard) Propose(command kvstore.Command) error {
	data, err := kvstore.EncodeCommand(command)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	if s.stopped || s.driver.Stopped() {
		return ErrStopped
	}

	// Never acknowledge a write on a follower.
	if s.node.Status().Role != raft.Leader {
		return raft.ErrNotLeader
	}

	_, found, err := s.store.CheckRequest(command)
	if err != nil {
		return err
	}
	if found {
		// Identical request was already applied.
		return nil
	}

	_, err = s.checkPendingRequest(command)
	if err != nil {
		return err
	}

	if err := s.node.Propose(data); err != nil {
		return err
	}

	return s.driver.Process()
}

func (s *Shard) Get(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return "", false, ErrClosed
	}
	if s.stopped || s.driver.Stopped() {
		return "", false, ErrStopped
	}

	value, exists := s.store.Get(key)
	return value, exists, nil
}

func (s *Shard) Status() (raft.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return raft.Status{}, ErrClosed
	}
	if s.stopped || s.driver.Stopped() {
		return raft.Status{}, ErrStopped
	}

	return s.node.Status(), nil
}

func (s *Shard) AppliedIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrClosed
	}
	if s.stopped || s.driver.Stopped() {
		return 0, ErrStopped
	}

	return s.store.AppliedIndex(), nil
}

func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	return s.storage.Close()
}
