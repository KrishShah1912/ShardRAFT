package client

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

func testShardMap() *shardraftpb.ShardMap {
	return &shardraftpb.ShardMap{
		Epoch:      1,
		TotalSlots: TotalSlots,
		Shards: []*shardraftpb.ShardInfo{
			{
				ShardId: 1,
				Epoch:   1,
				SlotRanges: []*shardraftpb.SlotRange{
					{
						StartSlot: 0,
						EndSlot:   TotalSlots - 1,
					},
				},
				Replicas: []*shardraftpb.ReplicaInfo{
					{
						NodeId:  1,
						Address: "127.0.0.1:9091",
					},
					{
						NodeId:  2,
						Address: "127.0.0.1:9092",
					},
					{
						NodeId:  3,
						Address: "127.0.0.1:9093",
					},
				},
				LeaderId: 1,
			},
		},
	}
}

func requireRoutingTable(
	t testing.TB,
	metadata *shardraftpb.ShardMap,
) *RoutingTable {
	t.Helper()

	table, err := NewRoutingTable(metadata)

	if err != nil {
		t.Fatal(err)
	}

	return table
}

func TestRoutingTableSingleShard(t *testing.T) {
	table := requireRoutingTable(t, testShardMap())

	if table.Epoch() != 1 {
		t.Fatalf("unexpected epoch: %d", table.Epoch())
	}

	if table.ShardCount() != 1 {
		t.Fatalf(
			"unexpected shard count: %d",
			table.ShardCount(),
		)
	}

	shard, err := table.Shard(1)

	if err != nil {
		t.Fatal(err)
	}

	if shard.GetShardId() != 1 ||
		shard.GetEpoch() != 1 ||
		shard.GetLeaderId() != 1 ||
		len(shard.GetReplicas()) != 3 {
		t.Fatalf("unexpected shard: %+v", shard)
	}
}

func TestRoutingTableAllSlots(t *testing.T) {
	table := requireRoutingTable(t, testShardMap())

	for slot := uint32(0); slot < TotalSlots; slot++ {
		id, err := table.ShardIDForSlot(slot)

		if err != nil || id != 1 {
			t.Fatalf(
				"slot=%d shard=%d err=%v",
				slot,
				id,
				err,
			)
		}
	}

	t.Logf(
		"validated all %d slots belong to shard 1",
		TotalSlots,
	)
}

func TestRoutingTableBoundarySlots(t *testing.T) {
	table := requireRoutingTable(t, testShardMap())

	slots := []uint32{
		0,
		1,
		16383,
		16384,
		32767,
		32768,
		49151,
		49152,
		65534,
		65535,
	}

	for _, slot := range slots {
		id, err := table.ShardIDForSlot(slot)

		if err != nil || id != 1 {
			t.Fatalf(
				"slot=%d shard=%d err=%v",
				slot,
				id,
				err,
			)
		}
	}
}

func TestRoutingTableKeyLookup(t *testing.T) {
	table := requireRoutingTable(t, testShardMap())

	keys := []string{
		"alpha",
		"beta",
		"hello",
		"user:123",
		"session:456",
		"你好",
		"🚀",
		"key\x00value",
	}

	for _, key := range keys {
		id, err := table.ShardIDForKey(key)

		if err != nil || id != 1 {
			t.Fatalf(
				"key=%q shard=%d err=%v",
				key,
				id,
				err,
			)
		}

		shard, err := table.Route(key)

		if err != nil || shard.GetShardId() != 1 {
			t.Fatalf(
				"key=%q shard=%v err=%v",
				key,
				shard,
				err,
			)
		}
	}
}

func TestRoutingTableInvalidSlot(t *testing.T) {
	table := requireRoutingTable(t, testShardMap())

	for _, slot := range []uint32{
		TotalSlots,
		TotalSlots + 1,
		^uint32(0),
	} {
		_, err := table.ShardIDForSlot(slot)

		if !errors.Is(err, ErrInvalidSlot) {
			t.Fatalf(
				"slot=%d unexpected error: %v",
				slot,
				err,
			)
		}
	}
}

func TestRoutingTableUnknownShard(t *testing.T) {
	table := requireRoutingTable(t, testShardMap())

	for _, id := range []uint64{0, 2, 999} {
		_, err := table.Shard(id)

		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf(
				"shard=%d unexpected error: %v",
				id,
				err,
			)
		}
	}
}

func TestRoutingTableNilMetadata(t *testing.T) {
	_, err := NewRoutingTable(nil)

	if !errors.Is(err, ErrInvalidShardMap) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRoutingTableInvalidMetadata(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*shardraftpb.ShardMap)
	}{
		{
			"ZeroEpoch",
			func(m *shardraftpb.ShardMap) {
				m.Epoch = 0
			},
		},
		{
			"WrongSlotCount",
			func(m *shardraftpb.ShardMap) {
				m.TotalSlots = 1024
			},
		},
		{
			"NoShards",
			func(m *shardraftpb.ShardMap) {
				m.Shards = nil
			},
		},
		{
			"NilShard",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0] = nil
			},
		},
		{
			"ZeroShardID",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].ShardId = 0
			},
		},
		{
			"DuplicateShard",
			func(m *shardraftpb.ShardMap) {
				m.Shards = append(
					m.Shards,
					m.Shards[0],
				)
			},
		},
		{
			"ZeroShardEpoch",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].Epoch = 0
			},
		},
		{
			"NoSlotRanges",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].SlotRanges = nil
			},
		},
		{
			"NilSlotRange",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].SlotRanges[0] = nil
			},
		},
		{
			"ReversedSlotRange",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].SlotRanges[0].StartSlot = 10
				m.Shards[0].SlotRanges[0].EndSlot = 5
			},
		},
		{
			"OutOfRangeSlot",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].SlotRanges[0].EndSlot =
					TotalSlots
			},
		},
		{
			"MissingFirstSlot",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].SlotRanges[0].StartSlot = 1
			},
		},
		{
			"MissingLastSlot",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].SlotRanges[0].EndSlot =
					TotalSlots - 2
			},
		},
		{
			"OverlappingRanges",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].SlotRanges = append(
					m.Shards[0].SlotRanges,
					&shardraftpb.SlotRange{
						StartSlot: 100,
						EndSlot:   200,
					},
				)
			},
		},
		{
			"NoReplicas",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].Replicas = nil
			},
		},
		{
			"NilReplica",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].Replicas[0] = nil
			},
		},
		{
			"ZeroReplicaID",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].Replicas[0].NodeId = 0
			},
		},
		{
			"DuplicateReplica",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].Replicas[1].NodeId = 1
			},
		},
		{
			"UnknownLeader",
			func(m *shardraftpb.ShardMap) {
				m.Shards[0].LeaderId = 999
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := testShardMap()
			tt.mutate(metadata)

			table, err := NewRoutingTable(metadata)

			if !errors.Is(err, ErrInvalidShardMap) {
				t.Fatalf(
					"unexpected error: %v",
					err,
				)
			}

			if table != nil {
				t.Fatal(
					"invalid metadata produced a routing table",
				)
			}
		})
	}
}

func TestRoutingTableUnknownLeader(t *testing.T) {
	metadata := testShardMap()
	metadata.Shards[0].LeaderId = 0

	table := requireRoutingTable(t, metadata)

	shard, err := table.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if shard.GetLeaderId() != 0 {
		t.Fatal("unknown leader was not preserved")
	}
}

func TestRoutingTableMissingEndpoint(t *testing.T) {
	metadata := testShardMap()
	metadata.Shards[0].Replicas[1].Address = ""

	table := requireRoutingTable(t, metadata)

	shard, err := table.Shard(1)

	if err != nil {
		t.Fatal(err)
	}

	if len(shard.GetReplicas()) != 3 {
		t.Fatal("replica membership changed")
	}

	if shard.GetReplicas()[1].GetAddress() != "" {
		t.Fatal("missing endpoint was not preserved")
	}
}

func TestRoutingTableInputIsolation(t *testing.T) {
	metadata := testShardMap()
	table := requireRoutingTable(t, metadata)

	metadata.Epoch = 999
	metadata.Shards[0].ShardId = 999
	metadata.Shards[0].Epoch = 999
	metadata.Shards[0].LeaderId = 999
	metadata.Shards[0].SlotRanges[0].EndSlot = 0
	metadata.Shards[0].Replicas[0].Address = "changed"

	shard, err := table.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if table.Epoch() != 1 ||
		shard.GetShardId() != 1 ||
		shard.GetEpoch() != 1 ||
		shard.GetLeaderId() != 1 ||
		shard.GetSlotRanges()[0].GetEndSlot() !=
			TotalSlots-1 ||
		shard.GetReplicas()[0].GetAddress() !=
			"127.0.0.1:9091" {
		t.Fatal("input mutation changed routing table")
	}
}

func TestRoutingTableResponseIsolation(t *testing.T) {
	table := requireRoutingTable(t, testShardMap())

	first, err := table.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	first.ShardId = 999
	first.Epoch = 999
	first.LeaderId = 999
	first.SlotRanges[0].EndSlot = 0
	first.Replicas[0].Address = "changed"

	second, err := table.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if second.GetShardId() != 1 ||
		second.GetEpoch() != 1 ||
		second.GetLeaderId() != 1 ||
		second.GetSlotRanges()[0].GetEndSlot() !=
			TotalSlots-1 ||
		second.GetReplicas()[0].GetAddress() !=
			"127.0.0.1:9091" {
		t.Fatal("response mutation changed routing table")
	}
}

func TestRoutingTableConcurrentLookup(t *testing.T) {
	table := requireRoutingTable(t, testShardMap())

	const (
		workers = 32
		lookups = 1000
	)

	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()

			for i := 0; i < lookups; i++ {
				key := fmt.Sprintf(
					"worker-%d-key-%d",
					worker,
					i,
				)

				shard, err := table.Route(key)

				if err != nil {
					t.Error(err)
					return
				}

				if shard.GetShardId() != 1 {
					t.Errorf(
						"unexpected shard for %q",
						key,
					)
					return
				}

				// Modify the returned copy to verify
				// isolation between concurrent callers.
				shard.LeaderId = 999
				shard.Replicas[0].Address = "changed"
			}
		}(worker)
	}

	wg.Wait()

	shard, err := table.Shard(1)

	if err != nil {
		t.Fatal(err)
	}

	if shard.GetLeaderId() != 1 ||
		shard.GetReplicas()[0].GetAddress() !=
			"127.0.0.1:9091" {
		t.Fatal("concurrent lookups changed metadata")
	}

	t.Logf(
		"validated %d concurrent routing lookups",
		workers*lookups,
	)
}

func BenchmarkRoutingTableSlotLookup(b *testing.B) {
	table := requireRoutingTable(b, testShardMap())

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = table.ShardIDForSlot(
			uint32(i) % TotalSlots,
		)
	}
}

func BenchmarkRoutingTableKeyLookup(b *testing.B) {
	table := requireRoutingTable(b, testShardMap())

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = table.ShardIDForKey(
			"user:123456789",
		)
	}
}

func BenchmarkRoutingTableRoute(b *testing.B) {
	table := requireRoutingTable(b, testShardMap())

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = table.Route("user:123456789")
	}
}
