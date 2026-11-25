package meta

import (
	"errors"
	"fmt"
	"sort"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// ErrInvalidShardMap is returned when a shard map fails validation.
var ErrInvalidShardMap = errors.New("invalid shard map")

// ErrSlotUnowned is returned when a slot has no owning shard.
var ErrSlotUnowned = errors.New("slot has no owner")

// ShardID identifies a replication group that owns a slot range.
type ShardID uint64

// SlotRange is an inclusive range of logical hash slots.
type SlotRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
}

// Shard describes a replication group and the slots it owns.
type Shard struct {
	ID       ShardID       `json:"id"`
	Range    SlotRange     `json:"range"`
	Replicas []raft.NodeID `json:"replicas"`
}

// ShardMap is a versioned assignment of every logical slot to a
// single owning shard.
type ShardMap struct {
	Version uint64  `json:"version"`
	Shards  []Shard `json:"shards"`
}

// DefaultShardMap returns the initial four-shard layout at version 1.
//
// The key space is split into four equal ranges, each replicated
// across three nodes:
//
//	S1: slots 0-16383      replicas n1, n2, n3
//	S2: slots 16384-32767  replicas n2, n3, n4
//	S3: slots 32768-49151  replicas n3, n4, n5
//	S4: slots 49152-65535  replicas n4, n5, n1
func DefaultShardMap() ShardMap {
	return ShardMap{
		Version: 1,
		Shards: []Shard{
			{
				ID:       1,
				Range:    SlotRange{Start: 0, End: 16383},
				Replicas: []raft.NodeID{1, 2, 3},
			},
			{
				ID:       2,
				Range:    SlotRange{Start: 16384, End: 32767},
				Replicas: []raft.NodeID{2, 3, 4},
			},
			{
				ID:       3,
				Range:    SlotRange{Start: 32768, End: 49151},
				Replicas: []raft.NodeID{3, 4, 5},
			},
			{
				ID:       4,
				Range:    SlotRange{Start: 49152, End: 65535},
				Replicas: []raft.NodeID{4, 5, 1},
			},
		},
	}
}

// Validate reports whether the shard map is well formed.
//
// A valid shard map has a nonzero version, shards with unique nonzero
// IDs and valid unique replicas, and slot ranges that are contiguous
// with no gaps or overlaps so that every slot is owned exactly once.
func (m ShardMap) Validate() error {
	if m.Version == 0 {
		return fmt.Errorf(
			"%w: version must be nonzero",
			ErrInvalidShardMap,
		)
	}

	if len(m.Shards) == 0 {
		return fmt.Errorf(
			"%w: no shards",
			ErrInvalidShardMap,
		)
	}

	seenShards := make(map[ShardID]bool, len(m.Shards))

	for _, shard := range m.Shards {
		if shard.ID == 0 {
			return fmt.Errorf(
				"%w: shard ID must be nonzero",
				ErrInvalidShardMap,
			)
		}

		if seenShards[shard.ID] {
			return fmt.Errorf(
				"%w: duplicate shard %d",
				ErrInvalidShardMap,
				shard.ID,
			)
		}

		seenShards[shard.ID] = true

		if err := validateReplicas(shard); err != nil {
			return err
		}

		start := shard.Range.Start
		end := shard.Range.End

		if start > end || end >= TotalSlots {
			return fmt.Errorf(
				"%w: shard %d has invalid range %d-%d",
				ErrInvalidShardMap,
				shard.ID,
				start,
				end,
			)
		}
	}

	return validateCoverage(m.Shards)
}

func validateReplicas(shard Shard) error {
	if len(shard.Replicas) == 0 {
		return fmt.Errorf(
			"%w: shard %d has no replicas",
			ErrInvalidShardMap,
			shard.ID,
		)
	}

	seen := make(map[raft.NodeID]bool, len(shard.Replicas))

	for _, replica := range shard.Replicas {
		if replica == 0 {
			return fmt.Errorf(
				"%w: shard %d has a zero replica ID",
				ErrInvalidShardMap,
				shard.ID,
			)
		}

		if seen[replica] {
			return fmt.Errorf(
				"%w: shard %d has duplicate replica %d",
				ErrInvalidShardMap,
				shard.ID,
				replica,
			)
		}

		seen[replica] = true
	}

	return nil
}

// validateCoverage checks that the shard ranges tile the entire slot
// space contiguously, with no gaps or overlaps.
func validateCoverage(shards []Shard) error {
	ordered := make([]Shard, len(shards))
	copy(ordered, shards)

	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Range.Start < ordered[j].Range.Start
	})

	var next uint32

	for _, shard := range ordered {
		if shard.Range.Start != next {
			if shard.Range.Start < next {
				return fmt.Errorf(
					"%w: overlapping ranges at slot %d",
					ErrInvalidShardMap,
					shard.Range.Start,
				)
			}

			return fmt.Errorf(
				"%w: gap before slot %d",
				ErrInvalidShardMap,
				shard.Range.Start,
			)
		}

		next = shard.Range.End + 1
	}

	if next != TotalSlots {
		return fmt.Errorf(
			"%w: ranges cover %d of %d slots",
			ErrInvalidShardMap,
			next,
			TotalSlots,
		)
	}

	return nil
}

// OwnerForSlot returns the shard that owns a logical slot.
func (m ShardMap) OwnerForSlot(slot uint32) (Shard, error) {
	for _, shard := range m.Shards {
		if slot >= shard.Range.Start && slot <= shard.Range.End {
			return shard.clone(), nil
		}
	}

	return Shard{}, fmt.Errorf("%w: %d", ErrSlotUnowned, slot)
}

// OwnerForKey hashes a key and returns its owning shard.
func (m ShardMap) OwnerForKey(key string) (Shard, error) {
	return m.OwnerForSlot(SlotForKey(key))
}

// Clone returns an independent copy of the shard map.
func (m ShardMap) Clone() ShardMap {
	if m.Shards == nil {
		return ShardMap{Version: m.Version}
	}

	shards := make([]Shard, len(m.Shards))
	for i, shard := range m.Shards {
		shards[i] = shard.clone()
	}

	return ShardMap{Version: m.Version, Shards: shards}
}

func (s Shard) clone() Shard {
	replicas := make([]raft.NodeID, len(s.Replicas))
	copy(replicas, s.Replicas)

	return Shard{
		ID:       s.ID,
		Range:    s.Range,
		Replicas: replicas,
	}
}
