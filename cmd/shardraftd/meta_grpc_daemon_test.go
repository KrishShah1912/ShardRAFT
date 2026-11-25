package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func metaDaemonClients(
	t *testing.T,
	nodes []*kvDaemonNode,
) map[uint64]shardraftpb.MetaServiceClient {
	t.Helper()

	clients := make(
		map[uint64]shardraftpb.MetaServiceClient,
		len(nodes),
	)

	for _, node := range nodes {
		conn, err := grpc.NewClient(
			node.cfg.grpcAddr,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)

		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		})

		clients[node.cfg.id] =
			shardraftpb.NewMetaServiceClient(conn)
	}

	return clients
}

func metaDaemonValidateMap(
	result *shardraftpb.ShardMap,
	nodes []*kvDaemonNode,
) error {
	if result == nil {
		return fmt.Errorf("missing shard map")
	}

	if result.GetEpoch() != kvInitialEpoch ||
		result.GetTotalSlots() != metaTotalSlots {
		return fmt.Errorf(
			"unexpected map configuration: %+v",
			result,
		)
	}

	if len(result.GetShards()) != 1 {
		return fmt.Errorf("expected exactly one shard")
	}

	shard := result.GetShards()[0]

	if shard.GetShardId() != defaultShardID ||
		shard.GetEpoch() != result.GetEpoch() {
		return fmt.Errorf(
			"unexpected shard identity: %+v",
			shard,
		)
	}

	ranges := shard.GetSlotRanges()

	if len(ranges) != 1 ||
		ranges[0].GetStartSlot() != 0 ||
		ranges[0].GetEndSlot() != metaTotalSlots-1 {
		return fmt.Errorf(
			"unexpected slot ownership: %+v",
			ranges,
		)
	}

	replicas := shard.GetReplicas()

	if len(replicas) != len(nodes) {
		return fmt.Errorf(
			"replica count=%d want=%d",
			len(replicas),
			len(nodes),
		)
	}

	expected := make(map[uint64]string)

	for _, node := range nodes {
		expected[node.cfg.id] = node.cfg.grpcAddr
	}

	var previous uint64

	for _, replica := range replicas {
		id := replica.GetNodeId()

		if id <= previous {
			return fmt.Errorf(
				"replicas are not uniquely ordered",
			)
		}

		address, found := expected[id]

		if !found ||
			replica.GetAddress() != address {
			return fmt.Errorf(
				"unexpected replica: %+v",
				replica,
			)
		}

		previous = id
	}

	if leaderID := shard.GetLeaderId(); leaderID != 0 {
		if _, found := expected[leaderID]; !found {
			return fmt.Errorf(
				"leader %d is not a replica",
				leaderID,
			)
		}
	}

	return nil
}

func metaDaemonWaitMap(
	t *testing.T,
	client shardraftpb.MetaServiceClient,
	nodes []*kvDaemonNode,
	expectedLeader uint64,
) *shardraftpb.ShardMap {
	t.Helper()

	deadline := time.Now().Add(12 * time.Second)

	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			500*time.Millisecond,
		)

		response, err := client.GetShardMap(
			ctx,
			&shardraftpb.GetShardMapRequest{},
		)

		cancel()

		if err != nil {
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}

		result := response.GetShardMap()

		if err := metaDaemonValidateMap(
			result,
			nodes,
		); err != nil {
			t.Fatal(err)
		}

		if result.GetShards()[0].GetLeaderId() ==
			expectedLeader {
			return result
		}

		lastErr = fmt.Errorf(
			"leader=%d want=%d",
			result.GetShards()[0].GetLeaderId(),
			expectedLeader,
		)

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf(
		"metadata did not converge: %v",
		lastErr,
	)

	return nil
}

func TestMetaGRPCDaemonReplicaDiscovery(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	clients := metaDaemonClients(t, nodes)

	leader := kvDaemonWaitLeader(t, nodes, 0)

	var reference *shardraftpb.ShardMap

	for _, node := range nodes {
		result := metaDaemonWaitMap(
			t,
			clients[node.cfg.id],
			nodes,
			leader.cfg.id,
		)

		if reference == nil {
			reference = result
		} else if !proto.Equal(reference, result) {
			t.Fatalf(
				"node %d returned inconsistent metadata",
				node.cfg.id,
			)
		}

		t.Logf(
			"node=%d leader=%d replicas=%d epoch=%d",
			node.cfg.id,
			result.GetShards()[0].GetLeaderId(),
			len(result.GetShards()[0].GetReplicas()),
			result.GetEpoch(),
		)
	}
}

func TestMetaGRPCDaemonShardLookup(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	clients := metaDaemonClients(t, nodes)

	leader := kvDaemonWaitLeader(t, nodes, 0)

	for _, node := range nodes {
		client := clients[node.cfg.id]

		expected := metaDaemonWaitMap(
			t,
			client,
			nodes,
			leader.cfg.id,
		)

		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		result, err := client.GetShard(
			ctx,
			&shardraftpb.GetShardRequest{
				ShardId: defaultShardID,
			},
		)

		cancel()

		if err != nil {
			t.Fatal(err)
		}

		if !result.GetFound() ||
			!proto.Equal(
				result.GetShard(),
				expected.GetShards()[0],
			) {
			t.Fatalf(
				"node %d returned inconsistent shard",
				node.cfg.id,
			)
		}

		ctx, cancel = context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		unknown, err := client.GetShard(
			ctx,
			&shardraftpb.GetShardRequest{
				ShardId: 999,
			},
		)

		cancel()

		if err != nil {
			t.Fatal(err)
		}

		if unknown.GetFound() ||
			unknown.GetShard() != nil {
			t.Fatalf(
				"node %d returned unknown shard",
				node.cfg.id,
			)
		}
	}
}

func TestMetaGRPCDaemonAutomaticFailover(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	clients := metaDaemonClients(t, nodes)

	oldLeader := kvDaemonWaitLeader(t, nodes, 0)

	before := metaDaemonWaitMap(
		t,
		clients[oldLeader.cfg.id],
		nodes,
		oldLeader.cfg.id,
	)

	oldStatus := kvDaemonStatus(oldLeader)

	if oldStatus == nil {
		t.Fatal("missing original leader status")
	}

	kvDaemonPut(
		t,
		oldLeader,
		"metadata-failover-write",
		"100",
	)

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			shardraftpb.ReadMode_READ_MODE_LOCAL,
			"100",
		)
	}

	oldLeader.stop(t)

	newLeader := kvDaemonWaitLeader(
		t,
		nodes,
		oldLeader.cfg.id,
	)

	if newLeader.cfg.id == oldLeader.cfg.id {
		t.Fatal("leader did not change")
	}

	newStatus := kvDaemonStatus(newLeader)

	if newStatus == nil ||
		newStatus.GetTerm() <= oldStatus.GetTerm() {
		t.Fatal("leadership term did not advance")
	}

	for _, node := range nodes {
		if node.cfg.id == oldLeader.cfg.id {
			continue
		}

		after := metaDaemonWaitMap(
			t,
			clients[node.cfg.id],
			nodes,
			newLeader.cfg.id,
		)

		if after.GetEpoch() != before.GetEpoch() {
			t.Fatal("failover changed routing epoch")
		}

		expected := proto.Clone(before).(*shardraftpb.ShardMap)
		expected.Shards[0].LeaderId = newLeader.cfg.id

		if !proto.Equal(expected, after) {
			t.Fatal(
				"failover unexpectedly changed shard ownership or membership",
			)
		}
	}

	kvDaemonWaitValue(
		t,
		newLeader,
		shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		"100",
	)

	kvDaemonPut(
		t,
		newLeader,
		"metadata-post-failover-write",
		"200",
	)

	kvDaemonWaitValue(
		t,
		newLeader,
		shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		"200",
	)

	t.Logf(
		"leader changed %d -> %d; term %d -> %d; epoch remained %d",
		oldLeader.cfg.id,
		newLeader.cfg.id,
		oldStatus.GetTerm(),
		newStatus.GetTerm(),
		before.GetEpoch(),
	)
}

func TestMetaGRPCDaemonConcurrentDiscovery(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	clients := metaDaemonClients(t, nodes)

	leader := kvDaemonWaitLeader(t, nodes, 0)

	for _, node := range nodes {
		metaDaemonWaitMap(
			t,
			clients[node.cfg.id],
			nodes,
			leader.cfg.id,
		)
	}

	const (
		readers = 12
		reads   = 100
		writes  = 20
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	var wg sync.WaitGroup

	start := make(chan struct{})
	errs := make(chan error, readers+1)

	for i := 0; i < readers; i++ {
		wg.Add(1)

		go func(reader int) {
			defer wg.Done()
			<-start

			node := nodes[reader%len(nodes)]
			client := clients[node.cfg.id]

			for j := 0; j < reads; j++ {
				response, err := client.GetShardMap(
					ctx,
					&shardraftpb.GetShardMapRequest{},
				)

				if err != nil {
					errs <- err
					return
				}

				if err := metaDaemonValidateMap(
					response.GetShardMap(),
					nodes,
				); err != nil {
					errs <- err
					return
				}

				shard, err := client.GetShard(
					ctx,
					&shardraftpb.GetShardRequest{
						ShardId: defaultShardID,
					},
				)

				if err != nil {
					errs <- err
					return
				}

				if !shard.GetFound() ||
					shard.GetShard().GetEpoch() !=
						kvInitialEpoch ||
					shard.GetShard().GetShardId() !=
						defaultShardID {
					errs <- fmt.Errorf(
						"reader %d received invalid shard",
						reader,
					)
					return
				}
			}
		}(i)
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < writes; i++ {
			response, err := leader.kv.Put(
				ctx,
				&shardraftpb.PutRequest{
					Header: kvDaemonHeader(
						fmt.Sprintf(
							"metadata-concurrent-%d",
							i,
						),
					),
					Key: "production-key",
					Value: fmt.Sprintf(
						"value-%d",
						i,
					),
				},
			)

			if err != nil {
				errs <- err
				return
			}

			if response == nil ||
				response.GetError() != nil ||
				!response.GetApplied() {
				errs <- fmt.Errorf(
					"unexpected mutation: %+v",
					response,
				)
				return
			}
		}
	}()

	close(start)

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	if t.Failed() {
		return
	}

	kvDaemonWaitValue(
		t,
		leader,
		shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		"value-19",
	)

	t.Logf(
		"validated %d GetShardMap RPCs, %d GetShard RPCs and %d KV mutations",
		readers*reads,
		readers*reads,
		writes,
	)
}

func TestMetaGRPCDaemonWithoutQuorum(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	clients := metaDaemonClients(t, nodes)

	leader := kvDaemonWaitLeader(t, nodes, 0)

	before := metaDaemonWaitMap(
		t,
		clients[leader.cfg.id],
		nodes,
		leader.cfg.id,
	)

	for _, node := range nodes {
		if node.cfg.id != leader.cfg.id {
			node.stop(t)
		}
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	response, err := clients[leader.cfg.id].GetShardMap(
		ctx,
		&shardraftpb.GetShardMapRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	after := response.GetShardMap()

	if err := metaDaemonValidateMap(
		after,
		nodes,
	); err != nil {
		t.Fatal(err)
	}

	if after.GetEpoch() != before.GetEpoch() {
		t.Fatal("quorum loss changed routing epoch")
	}

	expected := proto.Clone(before).(*shardraftpb.ShardMap)
	expected.Shards[0].LeaderId =
		after.GetShards()[0].GetLeaderId()

	if !proto.Equal(expected, after) {
		t.Fatal("quorum loss changed static ownership")
	}

	t.Logf(
		"metadata remains available without quorum; observed leader=%d epoch=%d",
		after.GetShards()[0].GetLeaderId(),
		after.GetEpoch(),
	)
}
