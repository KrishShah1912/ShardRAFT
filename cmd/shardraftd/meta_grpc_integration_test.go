package main

import (
	"testing"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMetaGRPCWireServiceRegistration(t *testing.T) {
	f := newAdminWireFixture(t)

	services := f.server.server.GetServiceInfo()

	for _, name := range []string{
		"shardraft.v1.RaftService",
		"shardraft.v1.AdminService",
		"shardraft.v1.KVService",
		"shardraft.v1.MetaService",
	} {
		if _, found := services[name]; !found {
			t.Fatalf("missing service: %s", name)
		}
	}

	meta := services["shardraft.v1.MetaService"]
	methods := make(map[string]bool)

	for _, method := range meta.Methods {
		methods[method.Name] = true

		if method.IsClientStream || method.IsServerStream {
			t.Fatalf("unexpected streaming method: %s", method.Name)
		}
	}

	if len(methods) != 2 ||
		!methods["GetShardMap"] ||
		!methods["GetShard"] {
		t.Fatalf("unexpected metadata methods: %v", methods)
	}

	t.Log("Raft, Admin, KV and Meta services share one listener")
}

func TestMetaGRPCWireInitialShardMap(t *testing.T) {
	f := newAdminWireFixture(t)

	client := shardraftpb.NewMetaServiceClient(f.conn)

	response, err := client.GetShardMap(
		adminWireContext(t),
		&shardraftpb.GetShardMapRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	result := response.GetShardMap()

	if result == nil ||
		result.GetEpoch() != kvInitialEpoch ||
		result.GetTotalSlots() != 65536 ||
		len(result.GetShards()) != 1 {
		t.Fatalf("unexpected shard map: %+v", result)
	}

	shard := result.GetShards()[0]

	if shard.GetShardId() != defaultShardID ||
		shard.GetEpoch() != kvInitialEpoch ||
		shard.GetLeaderId() != 0 {
		t.Fatalf("unexpected initial shard: %+v", shard)
	}

	ranges := shard.GetSlotRanges()

	if len(ranges) != 1 ||
		ranges[0].GetStartSlot() != 0 ||
		ranges[0].GetEndSlot() != 65535 {
		t.Fatalf("unexpected slot ranges: %+v", ranges)
	}

	replicas := shard.GetReplicas()

	if len(replicas) != 1 ||
		replicas[0].GetNodeId() != 1 ||
		replicas[0].GetAddress() != f.server.Addr().String() {
		t.Fatalf("unexpected replicas: %+v", replicas)
	}

	t.Logf(
		"advertised dynamic endpoint: %s",
		replicas[0].GetAddress(),
	)
}

func TestMetaGRPCWireLeaderDiscovery(t *testing.T) {
	f := newAdminWireFixture(t)
	client := shardraftpb.NewMetaServiceClient(f.conn)

	before, err := client.GetShardMap(
		adminWireContext(t),
		&shardraftpb.GetShardMapRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	electAdminWireLeader(t, f.node)

	after, err := client.GetShardMap(
		adminWireContext(t),
		&shardraftpb.GetShardMapRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	initial := before.GetShardMap()
	elected := after.GetShardMap()

	if len(initial.GetShards()) != 1 ||
		len(elected.GetShards()) != 1 {
		t.Fatal("unexpected shard count")
	}

	if initial.GetShards()[0].GetLeaderId() != 0 ||
		elected.GetShards()[0].GetLeaderId() != 1 {
		t.Fatal("metadata did not reflect leader election")
	}

	if initial.GetEpoch() != elected.GetEpoch() {
		t.Fatal("leader election changed routing epoch")
	}

	admin := requireAdminWireStatus(t, f)

	if elected.GetShards()[0].GetLeaderId() != admin.GetLeaderId() {
		t.Fatal("metadata leader differs from AdminService")
	}
}

func TestMetaGRPCWireGetShard(t *testing.T) {
	f := newAdminWireFixture(t)
	electAdminWireLeader(t, f.node)

	client := shardraftpb.NewMetaServiceClient(f.conn)

	result, err := client.GetShard(
		adminWireContext(t),
		&shardraftpb.GetShardRequest{
			ShardId: defaultShardID,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if !result.GetFound() || result.GetShard() == nil {
		t.Fatalf("missing known shard: %+v", result)
	}

	full, err := client.GetShardMap(
		adminWireContext(t),
		&shardraftpb.GetShardMapRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	shards := full.GetShardMap().GetShards()

	if len(shards) != 1 ||
		!proto.Equal(result.GetShard(), shards[0]) {
		t.Fatal("GetShard differs from GetShardMap")
	}
}

func TestMetaGRPCWireUnknownShard(t *testing.T) {
	f := newAdminWireFixture(t)
	client := shardraftpb.NewMetaServiceClient(f.conn)

	result, err := client.GetShard(
		adminWireContext(t),
		&shardraftpb.GetShardRequest{
			ShardId: 999,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if result == nil ||
		result.GetFound() ||
		result.GetShard() != nil {
		t.Fatalf("unexpected response: %+v", result)
	}
}

func TestMetaGRPCWireInvalidShard(t *testing.T) {
	f := newAdminWireFixture(t)
	client := shardraftpb.NewMetaServiceClient(f.conn)

	_, err := client.GetShard(
		adminWireContext(t),
		&shardraftpb.GetShardRequest{},
	)

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMetaGRPCWireClosedShard(t *testing.T) {
	f := newAdminWireFixture(t)
	client := shardraftpb.NewMetaServiceClient(f.conn)

	if err := f.node.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := client.GetShardMap(
		adminWireContext(t),
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}
}
