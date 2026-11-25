package meta

import (
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestDefaultShardMapIsValid(t *testing.T) {
	if err := DefaultShardMap().Validate(); err != nil {
		t.Fatalf("default shard map is invalid: %v", err)
	}
}

func TestDefaultShardMapEverySlotHasOneOwner(t *testing.T) {
	m := DefaultShardMap()

	for slot := uint32(0); slot < TotalSlots; slot++ {
		owner, err := m.OwnerForSlot(slot)
		if err != nil {
			t.Fatalf("slot %d has no owner: %v", slot, err)
		}

		count := 0
		for _, shard := range m.Shards {
			if slot >= shard.Range.Start && slot <= shard.Range.End {
				count++
			}
		}

		if count != 1 {
			t.Fatalf(
				"slot %d is owned by %d shards",
				slot,
				count,
			)
		}

		if owner.ID == 0 {
			t.Fatalf("slot %d has zero owner", slot)
		}
	}
}

func TestDefaultShardMapBoundaries(t *testing.T) {
	m := DefaultShardMap()

	cases := []struct {
		slot  uint32
		owner ShardID
	}{
		{0, 1},
		{16383, 1},
		{16384, 2},
		{32767, 2},
		{32768, 3},
		{49151, 3},
		{49152, 4},
		{65535, 4},
	}

	for _, tc := range cases {
		owner, err := m.OwnerForSlot(tc.slot)
		if err != nil {
			t.Fatalf("slot %d: %v", tc.slot, err)
		}

		if owner.ID != tc.owner {
			t.Fatalf(
				"slot %d owned by shard %d, want %d",
				tc.slot,
				owner.ID,
				tc.owner,
			)
		}
	}
}

func TestDefaultShardMapKeysSpreadAcrossShards(t *testing.T) {
	m := DefaultShardMap()

	seen := make(map[ShardID]int)

	for i := 0; i < 100000; i++ {
		key := "key:" + string(rune(i))

		owner, err := m.OwnerForKey(key)
		if err != nil {
			t.Fatalf("key %q: %v", key, err)
		}

		seen[owner.ID]++
	}

	for _, id := range []ShardID{1, 2, 3, 4} {
		if seen[id] == 0 {
			t.Fatalf("no keys mapped to shard %d", id)
		}
	}

	if len(seen) != 4 {
		t.Fatalf("keys mapped to %d shards, want 4", len(seen))
	}
}

func TestShardMapOutOfRangeSlot(t *testing.T) {
	m := DefaultShardMap()

	if _, err := m.OwnerForSlot(TotalSlots); !errors.Is(err, ErrSlotUnowned) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShardMapValidateRejectsInvalid(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ShardMap)
	}{
		{
			"ZeroVersion",
			func(m *ShardMap) {
				m.Version = 0
			},
		},
		{
			"NoShards",
			func(m *ShardMap) {
				m.Shards = nil
			},
		},
		{
			"ZeroShardID",
			func(m *ShardMap) {
				m.Shards[0].ID = 0
			},
		},
		{
			"DuplicateShardID",
			func(m *ShardMap) {
				m.Shards[1].ID = m.Shards[0].ID
			},
		},
		{
			"NoReplicas",
			func(m *ShardMap) {
				m.Shards[0].Replicas = nil
			},
		},
		{
			"ZeroReplica",
			func(m *ShardMap) {
				m.Shards[0].Replicas[0] = 0
			},
		},
		{
			"DuplicateReplica",
			func(m *ShardMap) {
				m.Shards[0].Replicas[1] = m.Shards[0].Replicas[0]
			},
		},
		{
			"ReversedRange",
			func(m *ShardMap) {
				m.Shards[0].Range = SlotRange{Start: 10, End: 5}
			},
		},
		{
			"OutOfRangeEnd",
			func(m *ShardMap) {
				m.Shards[3].Range.End = TotalSlots
			},
		},
		{
			"GapBetweenRanges",
			func(m *ShardMap) {
				m.Shards[0].Range.End = 16382
			},
		},
		{
			"OverlappingRanges",
			func(m *ShardMap) {
				m.Shards[1].Range.Start = 16000
			},
		},
		{
			"MissingFirstSlot",
			func(m *ShardMap) {
				m.Shards[0].Range.Start = 1
			},
		},
		{
			"MissingLastSlot",
			func(m *ShardMap) {
				m.Shards[3].Range.End = TotalSlots - 2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := DefaultShardMap()
			tt.mutate(&m)

			if err := m.Validate(); !errors.Is(err, ErrInvalidShardMap) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestShardMapCloneIsIndependent(t *testing.T) {
	m := DefaultShardMap()
	clone := m.Clone()

	clone.Version = 999
	clone.Shards[0].ID = 999
	clone.Shards[0].Range.End = 0
	clone.Shards[0].Replicas[0] = raft.NodeID(999)

	if m.Version != 1 ||
		m.Shards[0].ID != 1 ||
		m.Shards[0].Range.End != 16383 ||
		m.Shards[0].Replicas[0] != 1 {
		t.Fatal("clone mutation changed the original shard map")
	}
}
