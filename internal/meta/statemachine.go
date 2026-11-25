package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// CommandType identifies a metadata command.
type CommandType string

// SetShardMap installs a new shard map if the command's expected
// version matches the current state.
const SetShardMap CommandType = "SET_SHARD_MAP"

// Errors returned when a metadata command is rejected.
var (
	ErrUnknownCommand = errors.New("unknown metadata command")
	ErrStaleVersion   = errors.New("stale shard map version")
)

// Command is a metadata state-machine command.
//
// A SET_SHARD_MAP command carries the version it expects to be current
// and the new shard map to install. The new map's version must be the
// current version plus one.
type Command struct {
	Type            CommandType `json:"type"`
	ExpectedVersion uint64      `json:"expected_version"`
	ShardMap        ShardMap    `json:"shard_map"`
}

// EncodeCommand serializes a metadata command.
func EncodeCommand(c Command) ([]byte, error) {
	if c.Type != SetShardMap {
		return nil, fmt.Errorf("%w: %q", ErrUnknownCommand, c.Type)
	}

	return json.Marshal(c)
}

// DecodeCommand deserializes a metadata command.
func DecodeCommand(data []byte) (Command, error) {
	var c Command

	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}

	if c.Type != SetShardMap {
		return Command{}, fmt.Errorf("%w: %q", ErrUnknownCommand, c.Type)
	}

	return c, nil
}

// StateMachine applies metadata commands and holds the current shard
// map. It is safe for concurrent use.
type StateMachine struct {
	mu      sync.RWMutex
	current ShardMap
}

// NewStateMachine returns an empty metadata state machine at version 0.
func NewStateMachine() *StateMachine {
	return &StateMachine{}
}

// Apply applies a metadata command.
//
// A SET_SHARD_MAP command is accepted only when its expected version
// equals the current version, its new map's version equals the current
// version plus one, and the new map is valid. The state is left
// unchanged when a command is rejected.
func (sm *StateMachine) Apply(cmd Command) error {
	if cmd.Type != SetShardMap {
		return fmt.Errorf("%w: %q", ErrUnknownCommand, cmd.Type)
	}

	if err := cmd.ShardMap.Validate(); err != nil {
		return err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if cmd.ExpectedVersion != sm.current.Version {
		return fmt.Errorf(
			"%w: expected current version %d, have %d",
			ErrStaleVersion,
			cmd.ExpectedVersion,
			sm.current.Version,
		)
	}

	if cmd.ShardMap.Version != sm.current.Version+1 {
		return fmt.Errorf(
			"%w: new version %d is not current %d plus one",
			ErrStaleVersion,
			cmd.ShardMap.Version,
			sm.current.Version,
		)
	}

	sm.current = cmd.ShardMap.Clone()
	return nil
}

// Current returns an independent copy of the current shard map.
func (sm *StateMachine) Current() ShardMap {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.current.Clone()
}

// Version returns the current shard map version.
func (sm *StateMachine) Version() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.current.Version
}

// Snapshot serializes the current shard map for durable storage.
func (sm *StateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return json.Marshal(sm.current)
}

// Restore replaces the state with a previously taken snapshot.
//
// An empty snapshot restores the initial version-0 state. A non-empty
// snapshot must contain a valid shard map.
func (sm *StateMachine) Restore(data []byte) error {
	var restored ShardMap

	if len(data) != 0 {
		if err := json.Unmarshal(data, &restored); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}

		if err := restored.Validate(); err != nil {
			return err
		}
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.current = restored.Clone()
	return nil
}
