package meta

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/raftdriver"
	"github.com/KrishShah1912/shardraft/internal/wal"
)

const MetadataGroupID ShardID = 5

var (
	ErrGroupClosed   = errors.New("metadata group is closed")
	ErrGroupStopped  = errors.New("metadata group is stopped")
	ErrInvalidMember = errors.New("invalid metadata group member")
)

func MetadataReplicas() []raft.NodeID {
	return []raft.NodeID{1, 2, 3}
}

func IsMetadataReplica(id raft.NodeID) bool {
	for _, member := range MetadataReplicas() {
		if member == id {
			return true
		}
	}

	return false
}

type GroupConfig struct {
	ID              raft.NodeID
	DataDir         string
	ElectionTimeout int
	SegmentOptions  wal.SegmentOptions
	Send            raftdriver.SendFunc
}

type Group struct {
	mu sync.Mutex

	node    *raft.Raft
	storage *wal.SegmentedWAL
	driver  *raftdriver.Driver
	machine *StateMachine

	appliedIndex uint64
	closed       bool
}

func OpenGroup(cfg GroupConfig) (*Group, error) {
	if !IsMetadataReplica(cfg.ID) {
		return nil, fmt.Errorf(
			"%w: node %d",
			ErrInvalidMember,
			cfg.ID,
		)
	}

	if cfg.DataDir == "" {
		return nil, errors.New("metadata data directory is required")
	}

	if cfg.ElectionTimeout <= 0 {
		return nil, errors.New("election timeout must be positive")
	}

	if cfg.Send == nil {
		return nil, errors.New("send callback is required")
	}

	peers := make([]raft.NodeID, 0, 2)

	for _, id := range MetadataReplicas() {
		if id != cfg.ID {
			peers = append(peers, id)
		}
	}

	dir := filepath.Join(cfg.DataDir, "metadata")

	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf(
			"create metadata directory: %w", err,
		)
	}

	g := &Group{
		machine: NewStateMachine(),
	}

	node, storage, driver, err :=
		raftdriver.RecoverSegmentedDriver(
			filepath.Join(dir, "raft.wal"),
			cfg.SegmentOptions,
			cfg.ID,
			peers,
			cfg.ElectionTimeout,
			g.applyEntry,
			cfg.Send,
		)

	if err != nil {
		return nil, err
	}

	g.node = node
	g.storage = storage
	g.driver = driver

	if node.SnapshotMetadata().LastIncludedIndex != 0 {
		_ = storage.Close()

		return nil, errors.New(
			"compacted metadata WAL is unsupported",
		)
	}

	if err := driver.Process(); err != nil {
		_ = storage.Close()

		return nil, fmt.Errorf(
			"replay committed metadata: %w", err,
		)
	}

	return g, nil
}

// applyEntry is called by the serialized Raft driver.
// Rejected metadata commands are deterministic no-ops.
func (g *Group) applyEntry(entry raft.Entry) error {
	if entry.Index <= g.appliedIndex {
		return nil
	}

	if entry.Index != g.appliedIndex+1 {
		return fmt.Errorf(
			"metadata application gap: applied=%d entry=%d",
			g.appliedIndex,
			entry.Index,
		)
	}

	switch entry.Type {
	case raft.EntryNoop:
		// Election no-op.

	case raft.EntryNormal:
		cmd, err := DecodeCommand(entry.Data)
		if err != nil {
			return err
		}

		err = g.machine.Apply(cmd)

		if err != nil &&
			!errors.Is(err, ErrStaleVersion) &&
			!errors.Is(err, ErrInvalidShardMap) &&
			!errors.Is(err, ErrUnknownCommand) {
			return err
		}

	default:
		return fmt.Errorf(
			"unsupported metadata entry type: %v",
			entry.Type,
		)
	}

	g.appliedIndex = entry.Index

	return nil
}

func (g *Group) available() error {
	if g.closed {
		return ErrGroupClosed
	}

	if g.driver.Stopped() {
		return ErrGroupStopped
	}

	return nil
}

func (g *Group) Tick() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return err
	}

	g.node.Tick()

	return g.driver.Process()
}

func (g *Group) Step(msg raft.Message) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return err
	}

	id := g.node.Status().ID

	if msg.To != id ||
		msg.From == id ||
		!IsMetadataReplica(msg.From) {
		return fmt.Errorf(
			"%w: sender=%d destination=%d",
			ErrInvalidMember,
			msg.From,
			msg.To,
		)
	}

	if err := g.node.Step(msg); err != nil {
		return err
	}

	return g.driver.Process()
}

// Propose submits a metadata command to Raft.
// Success means submission, not necessarily commitment.
func (g *Group) Propose(cmd Command) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return err
	}

	if g.node.Status().Role != raft.Leader {
		return raft.ErrNotLeader
	}

	data, err := EncodeCommand(cmd)
	if err != nil {
		return err
	}

	if err := cmd.ShardMap.Validate(); err != nil {
		return err
	}

	current := g.machine.Version()

	if cmd.ExpectedVersion != current ||
		cmd.ShardMap.Version != current+1 {
		return ErrStaleVersion
	}

	if err := g.node.Propose(data); err != nil {
		return err
	}

	return g.driver.Process()
}

// Bootstrap submits the initial map through Raft.
// It does not install the map directly into local state.
func (g *Group) Bootstrap() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return err
	}

	if g.node.Status().Role != raft.Leader {
		return raft.ErrNotLeader
	}

	if g.machine.Version() != 0 {
		return nil
	}

	data, err := EncodeCommand(Command{
		Type:            SetShardMap,
		ExpectedVersion: 0,
		ShardMap:        DefaultShardMap(),
	})

	if err != nil {
		return err
	}

	if err := g.node.Propose(data); err != nil {
		return err
	}

	return g.driver.Process()
}

// Current returns a copy of the locally applied metadata.
// It is not a quorum-confirmed linearizable read.
func (g *Group) Current() (ShardMap, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return ShardMap{}, err
	}

	return g.machine.Current(), nil
}

func (g *Group) Version() (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return 0, err
	}

	return g.machine.Version(), nil
}

func (g *Group) Status() (raft.Status, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return raft.Status{}, err
	}

	return g.node.Status(), nil
}

func (g *Group) StatusSnapshot() (
	raft.Status,
	uint64,
	error,
) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return raft.Status{}, 0, err
	}

	return g.node.Status(), g.appliedIndex, nil
}

func (g *Group) AppliedIndex() (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.available(); err != nil {
		return 0, err
	}

	return g.appliedIndex, nil
}

func (g *Group) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.closed {
		return nil
	}

	g.closed = true

	return g.storage.Close()
}
