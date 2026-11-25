package client

import (
	"errors"
	"fmt"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidShardMap = errors.New("invalid shard map")
	ErrInvalidSlot     = errors.New("invalid hash slot")
	ErrShardNotFound   = errors.New("shard not found")
)

// RoutingTable represents an immutable, validated shard map.
type RoutingTable struct {
	epoch  uint64
	owners [TotalSlots]uint64
	shards map[uint64]*shardraftpb.ShardInfo
}

// NewRoutingTable validates metadata and constructs a
// constant-time slot-to-shard lookup table.
func NewRoutingTable(
	metadata *shardraftpb.ShardMap,
) (*RoutingTable, error) {
	if metadata == nil {
		return nil, fmt.Errorf(
			"%w: missing metadata",
			ErrInvalidShardMap,
		)
	}

	if metadata.GetEpoch() == 0 {
		return nil, fmt.Errorf(
			"%w: zero routing epoch",
			ErrInvalidShardMap,
		)
	}

	if metadata.GetTotalSlots() != TotalSlots {
		return nil, fmt.Errorf(
			"%w: unexpected slot count %d",
			ErrInvalidShardMap,
			metadata.GetTotalSlots(),
		)
	}

	if len(metadata.GetShards()) == 0 {
		return nil, fmt.Errorf(
			"%w: no shards",
			ErrInvalidShardMap,
		)
	}

	table := &RoutingTable{
		epoch: metadata.GetEpoch(),
		shards: make(
			map[uint64]*shardraftpb.ShardInfo,
		),
	}

	for _, shard := range metadata.GetShards() {
		if err := table.addShard(shard); err != nil {
			return nil, err
		}
	}

	for slot, owner := range table.owners {
		if owner == 0 {
			return nil, fmt.Errorf(
				"%w: slot %d has no owner",
				ErrInvalidShardMap,
				slot,
			)
		}
	}

	return table, nil
}

func (table *RoutingTable) addShard(
	shard *shardraftpb.ShardInfo,
) error {
	if shard == nil || shard.GetShardId() == 0 {
		return fmt.Errorf(
			"%w: missing or zero shard ID",
			ErrInvalidShardMap,
		)
	}

	id := shard.GetShardId()

	if _, exists := table.shards[id]; exists {
		return fmt.Errorf(
			"%w: duplicate shard %d",
			ErrInvalidShardMap,
			id,
		)
	}

	if shard.GetEpoch() == 0 {
		return fmt.Errorf(
			"%w: shard %d has zero epoch",
			ErrInvalidShardMap,
			id,
		)
	}

	if err := validateShardReplicas(shard); err != nil {
		return err
	}

	if len(shard.GetSlotRanges()) == 0 {
		return fmt.Errorf(
			"%w: shard %d has no slot ranges",
			ErrInvalidShardMap,
			id,
		)
	}

	for _, slotRange := range shard.GetSlotRanges() {
		if slotRange == nil {
			return fmt.Errorf(
				"%w: nil slot range",
				ErrInvalidShardMap,
			)
		}

		start := slotRange.GetStartSlot()
		end := slotRange.GetEndSlot()

		if start > end || end >= TotalSlots {
			return fmt.Errorf(
				"%w: invalid range %d-%d",
				ErrInvalidShardMap,
				start,
				end,
			)
		}

		for slot := start; slot <= end; slot++ {
			if table.owners[slot] != 0 {
				return fmt.Errorf(
					"%w: overlapping ownership at slot %d",
					ErrInvalidShardMap,
					slot,
				)
			}

			table.owners[slot] = id
		}
	}

	table.shards[id] =
		proto.Clone(shard).(*shardraftpb.ShardInfo)

	return nil
}

func validateShardReplicas(
	shard *shardraftpb.ShardInfo,
) error {
	if len(shard.GetReplicas()) == 0 {
		return fmt.Errorf(
			"%w: shard %d has no replicas",
			ErrInvalidShardMap,
			shard.GetShardId(),
		)
	}

	seen := make(map[uint64]bool)

	for _, replica := range shard.GetReplicas() {
		if replica == nil || replica.GetNodeId() == 0 {
			return fmt.Errorf(
				"%w: invalid replica",
				ErrInvalidShardMap,
			)
		}

		id := replica.GetNodeId()

		if seen[id] {
			return fmt.Errorf(
				"%w: duplicate replica %d",
				ErrInvalidShardMap,
				id,
			)
		}

		seen[id] = true
	}

	if leader := shard.GetLeaderId(); leader != 0 {
		if !seen[leader] {
			return fmt.Errorf(
				"%w: leader %d is not a replica",
				ErrInvalidShardMap,
				leader,
			)
		}
	}

	return nil
}

// Epoch returns the global routing configuration epoch.
func (table *RoutingTable) Epoch() uint64 {
	return table.epoch
}

// ShardCount returns the number of discovered shards.
func (table *RoutingTable) ShardCount() int {
	return len(table.shards)
}

// ShardIDForSlot returns the owner of a logical slot.
func (table *RoutingTable) ShardIDForSlot(
	slot uint32,
) (uint64, error) {
	if slot >= TotalSlots {
		return 0, fmt.Errorf(
			"%w: %d",
			ErrInvalidSlot,
			slot,
		)
	}

	id := table.owners[slot]

	if id == 0 {
		return 0, ErrShardNotFound
	}

	return id, nil
}

// ShardIDForKey hashes a key and returns its owning shard.
func (table *RoutingTable) ShardIDForKey(
	key string,
) (uint64, error) {
	return table.ShardIDForSlot(HashSlot(key))
}

// Shard returns an independent copy of shard metadata.
func (table *RoutingTable) Shard(
	id uint64,
) (*shardraftpb.ShardInfo, error) {
	shard, exists := table.shards[id]

	if !exists {
		return nil, fmt.Errorf(
			"%w: %d",
			ErrShardNotFound,
			id,
		)
	}

	return proto.Clone(shard).(*shardraftpb.ShardInfo), nil
}

// Route resolves a key to its owning shard metadata.
func (table *RoutingTable) Route(
	key string,
) (*shardraftpb.ShardInfo, error) {
	id, err := table.ShardIDForKey(key)

	if err != nil {
		return nil, err
	}

	return table.Shard(id)
}
