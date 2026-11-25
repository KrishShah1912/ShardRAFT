package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type metaTestNode struct {
	state   raft.Status
	applied uint64
	err     error
}

func (n *metaTestNode) StatusSnapshot() (raft.Status, uint64, error) {
	return n.state, n.applied, n.err
}

func newMetaTestServer() (*metaGRPCServer, *metaTestNode) {
	node := &metaTestNode{
		state: raft.Status{
			ID:       1,
			Role:     raft.Leader,
			Term:     1,
			LeaderID: 1,
			Commit:   10,
		},
		applied: 10,
	}

	server := newMetaGRPCServer(
		defaultShardID,
		node,
		[]raft.NodeID{3, 2},
		map[raft.NodeID]string{
			2: "127.0.0.1:9092",
			3: "127.0.0.1:9093",
		},
		"127.0.0.1:9091",
	)

	return server, node
}

func requireMetaTestMap(
	t *testing.T,
	server *metaGRPCServer,
) *shardraftpb.ShardMap {
	t.Helper()

	response, err := server.GetShardMap(
		context.Background(),
		&shardraftpb.GetShardMapRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	if response == nil || response.ShardMap == nil {
		t.Fatal("missing shard map")
	}

	return response.ShardMap
}

func TestMetaGRPCStaticShardMap(t *testing.T) {
	server, _ := newMetaTestServer()
	result := requireMetaTestMap(t, server)

	if result.Epoch != kvInitialEpoch {
		t.Fatalf("unexpected map epoch: %d", result.Epoch)
	}

	if result.TotalSlots != 65536 {
		t.Fatalf("unexpected slot count: %d", result.TotalSlots)
	}

	if len(result.Shards) != 1 {
		t.Fatalf("unexpected shard count: %d", len(result.Shards))
	}

	shard := result.Shards[0]

	if shard.ShardId != defaultShardID ||
		shard.Epoch != result.Epoch {
		t.Fatalf("unexpected shard identity: %+v", shard)
	}

	if len(shard.SlotRanges) != 1 {
		t.Fatal("expected one slot range")
	}

	r := shard.SlotRanges[0]

	if r.StartSlot != 0 || r.EndSlot != 65535 {
		t.Fatalf("unexpected slot range: %+v", r)
	}

	if r.EndSlot-r.StartSlot+1 != result.TotalSlots {
		t.Fatal("slot coverage does not match total slots")
	}

	t.Logf(
		"shard=%d epoch=%d slots=%d range=%d-%d",
		shard.ShardId,
		shard.Epoch,
		result.TotalSlots,
		r.StartSlot,
		r.EndSlot,
	)
}

func TestMetaGRPCReplicaMembership(t *testing.T) {
	server, _ := newMetaTestServer()
	shard := requireMetaTestMap(t, server).Shards[0]

	if len(shard.Replicas) != 3 {
		t.Fatalf("unexpected replicas: %+v", shard.Replicas)
	}

	expected := []string{
		"127.0.0.1:9091",
		"127.0.0.1:9092",
		"127.0.0.1:9093",
	}

	for i, replica := range shard.Replicas {
		if replica.NodeId != uint64(i+1) {
			t.Fatalf("unexpected replica order: %+v", replica)
		}

		if replica.Address != expected[i] {
			t.Fatalf("unexpected replica address: %+v", replica)
		}
	}
}

func TestMetaGRPCGetKnownShard(t *testing.T) {
	server, _ := newMetaTestServer()

	response, err := server.GetShard(
		context.Background(),
		&shardraftpb.GetShardRequest{
			ShardId: defaultShardID,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if response == nil ||
		!response.Found ||
		response.Shard == nil {
		t.Fatalf("unexpected response: %+v", response)
	}

	expected := requireMetaTestMap(t, server).Shards[0]

	if !reflect.DeepEqual(response.Shard, expected) {
		t.Fatal("GetShard differs from GetShardMap")
	}
}

func TestMetaGRPCGetUnknownShard(t *testing.T) {
	server, _ := newMetaTestServer()

	response, err := server.GetShard(
		context.Background(),
		&shardraftpb.GetShardRequest{
			ShardId: 999,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if response == nil ||
		response.Found ||
		response.Shard != nil {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestMetaGRPCInvalidRequests(t *testing.T) {
	server, _ := newMetaTestServer()
	ctx := context.Background()

	t.Run("NilShardMapRequest", func(t *testing.T) {
		_, err := server.GetShardMap(ctx, nil)

		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("NilShardRequest", func(t *testing.T) {
		_, err := server.GetShard(ctx, nil)

		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ZeroShardID", func(t *testing.T) {
		_, err := server.GetShard(
			ctx,
			&shardraftpb.GetShardRequest{},
		)

		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestMetaGRPCLeaderDiscovery(t *testing.T) {
	tests := []struct {
		name     string
		role     raft.Role
		leaderID raft.NodeID
		expected uint64
	}{
		{"LocalLeader", raft.Leader, 1, 1},
		{"KnownFollowerLeader", raft.Follower, 2, 2},
		{"UnknownFollowerLeader", raft.Follower, 0, 0},
		{"Candidate", raft.Candidate, 2, 0},
		{"UnknownMemberLeader", raft.Follower, 99, 0},
		{"LocalLeaderWithoutHint", raft.Leader, 0, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, node := newMetaTestServer()

			node.state.Role = tc.role
			node.state.LeaderID = tc.leaderID

			result := requireMetaTestMap(t, server)

			if result.Shards[0].LeaderId != tc.expected {
				t.Fatalf(
					"leader=%d want=%d",
					result.Shards[0].LeaderId,
					tc.expected,
				)
			}
		})
	}
}

func TestMetaGRPCEpochStableAcrossLeaderChanges(t *testing.T) {
	server, node := newMetaTestServer()

	before := requireMetaTestMap(t, server)

	node.state.Role = raft.Follower
	node.state.LeaderID = 2
	node.state.Term++

	after := requireMetaTestMap(t, server)

	if before.Epoch != after.Epoch ||
		before.Shards[0].Epoch != after.Shards[0].Epoch {
		t.Fatal("leadership change modified routing epoch")
	}

	if before.Shards[0].LeaderId != 1 ||
		after.Shards[0].LeaderId != 2 {
		t.Fatal("leader metadata did not update")
	}

	t.Logf(
		"leader changed from %d to %d; epoch remained %d",
		before.Shards[0].LeaderId,
		after.Shards[0].LeaderId,
		after.Epoch,
	)
}

func TestMetaGRPCMissingPeerAddresses(t *testing.T) {
	_, node := newMetaTestServer()

	server := newMetaGRPCServer(
		defaultShardID,
		node,
		[]raft.NodeID{2, 3},
		map[raft.NodeID]string{
			2: "127.0.0.1:9092",
		},
		"127.0.0.1:9091",
	)

	replicas := requireMetaTestMap(t, server).Shards[0].Replicas

	if len(replicas) != 3 {
		t.Fatal("missing endpoint removed replica membership")
	}

	if replicas[2].NodeId != 3 ||
		replicas[2].Address != "" {
		t.Fatalf("unexpected replica: %+v", replicas[2])
	}
}

func TestMetaGRPCReplicaDeduplication(t *testing.T) {
	_, node := newMetaTestServer()

	server := newMetaGRPCServer(
		defaultShardID,
		node,
		[]raft.NodeID{3, 2, 2, 0, 1, 3},
		map[raft.NodeID]string{
			1:  "incorrect-local-address",
			2:  "127.0.0.1:9092",
			3:  "127.0.0.1:9093",
			99: "unexpected-member",
		},
		"127.0.0.1:9091",
	)

	replicas := requireMetaTestMap(t, server).Shards[0].Replicas

	if len(replicas) != 3 {
		t.Fatalf("unexpected replica count: %d", len(replicas))
	}

	for i, replica := range replicas {
		if replica.NodeId != uint64(i+1) {
			t.Fatalf("unexpected replica: %+v", replica)
		}
	}

	if replicas[0].Address != "127.0.0.1:9091" {
		t.Fatal("local endpoint was overwritten")
	}
}

func TestMetaGRPCConstructorIsolation(t *testing.T) {
	_, node := newMetaTestServer()

	peers := []raft.NodeID{2, 3}

	addresses := map[raft.NodeID]string{
		2: "127.0.0.1:9092",
		3: "127.0.0.1:9093",
	}

	server := newMetaGRPCServer(
		defaultShardID,
		node,
		peers,
		addresses,
		"127.0.0.1:9091",
	)

	peers[0] = 99
	addresses[2] = "modified-address"
	delete(addresses, 3)

	replicas := requireMetaTestMap(t, server).Shards[0].Replicas

	if len(replicas) != 3 ||
		replicas[1].NodeId != 2 ||
		replicas[1].Address != "127.0.0.1:9092" ||
		replicas[2].Address != "127.0.0.1:9093" {
		t.Fatal("constructor retained mutable caller data")
	}
}

func TestMetaGRPCResponseIsolation(t *testing.T) {
	server, _ := newMetaTestServer()

	first := requireMetaTestMap(t, server)

	first.Epoch = 999
	first.TotalSlots = 1
	first.Shards[0].Epoch = 999
	first.Shards[0].LeaderId = 999
	first.Shards[0].SlotRanges[0].EndSlot = 1
	first.Shards[0].Replicas[0].Address = "modified"

	second := requireMetaTestMap(t, server)

	if second.Epoch != kvInitialEpoch ||
		second.TotalSlots != 65536 ||
		second.Shards[0].Epoch != kvInitialEpoch ||
		second.Shards[0].LeaderId != 1 ||
		second.Shards[0].SlotRanges[0].EndSlot != 65535 ||
		second.Shards[0].Replicas[0].Address != "127.0.0.1:9091" {
		t.Fatal("metadata response mutation affected server state")
	}
}

func TestMetaGRPCUnavailableNode(t *testing.T) {
	server, node := newMetaTestServer()

	node.err = errors.New("node closed")

	_, err := server.GetShardMap(
		context.Background(),
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = server.GetShard(
		context.Background(),
		&shardraftpb.GetShardRequest{
			ShardId: defaultShardID,
		},
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMetaGRPCNilNode(t *testing.T) {
	server := newMetaGRPCServer(
		defaultShardID,
		nil,
		nil,
		nil,
		"",
	)

	_, err := server.GetShardMap(
		context.Background(),
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMetaGRPCInconsistentStatus(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*metaTestNode)
	}{
		{
			"ZeroNodeID",
			func(n *metaTestNode) {
				n.state.ID = 0
			},
		},
		{
			"AppliedBeyondCommit",
			func(n *metaTestNode) {
				n.applied = n.state.Commit + 1
			},
		},
		{
			"InvalidRole",
			func(n *metaTestNode) {
				n.state.Role = raft.Role(255)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, node := newMetaTestServer()

			tc.mutate(node)

			_, err := server.GetShardMap(
				context.Background(),
				&shardraftpb.GetShardMapRequest{},
			)

			if status.Code(err) != codes.Internal {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestMetaGRPCCanceledContext(t *testing.T) {
	server, _ := newMetaTestServer()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := server.GetShardMap(
		ctx,
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = server.GetShard(
		ctx,
		&shardraftpb.GetShardRequest{
			ShardId: defaultShardID,
		},
	)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMetaGRPCExpiredDeadline(t *testing.T) {
	server, _ := newMetaTestServer()

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	_, err := server.GetShardMap(
		ctx,
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = server.GetShard(
		ctx,
		&shardraftpb.GetShardRequest{
			ShardId: defaultShardID,
		},
	)

	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("unexpected error: %v", err)
	}
}
