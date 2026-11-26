package main

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	"github.com/KrishShah1912/shardraft/internal/transport"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestMultiMetaGRPCUnavailable(t *testing.T) {
	server := newMultiMetaGRPCServer(
		nil,
		nil,
		nil,
	)

	ctx := context.Background()

	_, err := server.GetShardMap(
		ctx,
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = server.GetShardMap(ctx, nil)

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = server.GetShard(
		ctx,
		&shardraftpb.GetShardRequest{},
	)

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Log("unavailable metadata and invalid requests rejected")
}

func TestMultiMetaGRPCDiscovery(t *testing.T) {
	addresses := make(map[raft.NodeID]string)

	var reservations []net.Listener

	for id := raft.NodeID(1); id <= 5; id++ {
		listener, err := net.Listen(
			"tcp",
			"127.0.0.1:0",
		)

		if err != nil {
			for _, existing := range reservations {
				_ = existing.Close()
			}
			t.Fatal(err)
		}

		reservations = append(
			reservations,
			listener,
		)

		addresses[id] = listener.Addr().String()
	}

	for _, listener := range reservations {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}

	runtimes := make(
		map[raft.NodeID]*metadataRuntime,
	)

	servers := make(
		map[raft.NodeID]*daemonGRPCServer,
	)

	managers := make(
		map[raft.NodeID]*shard.Manager,
	)

	t.Cleanup(func() {
		for _, server := range servers {
			server.Close()
		}

		for _, runtime := range runtimes {
			if err := runtime.Close(); err != nil {
				t.Error(err)
			}
		}

		for _, manager := range managers {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})

	for _, id := range meta.MetadataReplicas() {
		cfg := multiDaemonTestConfig(t)
		cfg.id = uint64(id)
		cfg.electionTimeout = 10 + int(id)*10

		var remote []transport.Peer

		for peer, address := range addresses {
			if peer == id {
				continue
			}

			remote = append(
				remote,
				transport.Peer{
					ID:      peer,
					Address: address,
				},
			)
		}

		runtime, err := openMetadataRuntime(
			cfg,
			remote,
		)

		if err != nil {
			t.Fatal(err)
		}

		runtimes[id] = runtime

		manager, err := shard.OpenManager(
			shard.ManagerConfig{
				NodeID:          id,
				DataDir:         cfg.dataDir,
				ElectionTimeout: cfg.electionTimeout,
				ShardMap:        meta.DefaultShardMap(),
				Send: func(
					meta.ShardID,
					[]raft.Message,
				) error {
					return nil
				},
			},
		)

		if err != nil {
			t.Fatal(err)
		}

		managers[id] = manager

		server, err :=
			startMultiRaftGRPCServerWithAddresses(
				addresses[id],
				manager,
				id,
				addresses,
				runtime.Group(),
			)

		if err != nil {
			t.Fatal(err)
		}

		servers[id] = server
	}

	t.Log("three metadata replicas started with real gRPC")

	// Before bootstrap, discovery must not expose
	// an uncommitted default shard map.
	initial := newMultiMetaGRPCServer(
		managers[1],
		runtimes[1].Group(),
		addresses,
	)

	_, err := initial.GetShardMap(
		context.Background(),
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf(
			"uncommitted metadata exposed: %v",
			err,
		)
	}

	deadline := time.Now().Add(10 * time.Second)
	ready := false

	for time.Now().Before(deadline) {
		for _, id := range meta.MetadataReplicas() {
			if err := runtimes[id].Tick(); err != nil {
				t.Fatalf(
					"metadata tick node=%d: %v",
					id,
					err,
				)
			}
		}

		ready = true

		for _, runtime := range runtimes {
			version, err := runtime.Group().Version()

			if err != nil {
				t.Fatal(err)
			}

			if version != 1 {
				ready = false
			}
		}

		if ready {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if !ready {
		t.Fatal("automatic metadata bootstrap timed out")
	}

	t.Log("automatic bootstrap committed on all three replicas")

	expected := meta.DefaultShardMap()

	for _, id := range meta.MetadataReplicas() {
		t.Run(
			fmt.Sprintf("replica-%d", id),
			func(t *testing.T) {
				conn, err := grpc.NewClient(
					addresses[id],
					grpc.WithTransportCredentials(
						insecure.NewCredentials(),
					),
				)

				if err != nil {
					t.Fatal(err)
				}

				defer conn.Close()

				client := shardraftpb.NewMetaServiceClient(
					conn,
				)

				ctx, cancel := context.WithTimeout(
					context.Background(),
					3*time.Second,
				)

				defer cancel()

				response, err := client.GetShardMap(
					ctx,
					&shardraftpb.GetShardMapRequest{},
				)

				if err != nil {
					t.Fatal(err)
				}

				layout := response.GetShardMap()

				if layout == nil ||
					layout.GetEpoch() != 1 ||
					layout.GetTotalSlots() != 65536 ||
					len(layout.GetShards()) != 4 {
					t.Fatalf(
						"invalid metadata response: %v",
						layout,
					)
				}

				for i, placement := range expected.Shards {
					info := layout.GetShards()[i]

					if info.GetShardId() != uint64(placement.ID) ||
						info.GetEpoch() != 1 {
						t.Fatalf(
							"incorrect shard identity: %v",
							info,
						)
					}

					ranges := info.GetSlotRanges()

					if len(ranges) != 1 ||
						ranges[0].GetStartSlot() != placement.Range.Start ||
						ranges[0].GetEndSlot() != placement.Range.End {
						t.Fatalf(
							"incorrect slot range: %v",
							info,
						)
					}

					actual := make(
						map[raft.NodeID]string,
					)

					for _, replica := range info.GetReplicas() {
						actual[raft.NodeID(replica.GetNodeId())] =
							replica.GetAddress()
					}

					want := make(
						map[raft.NodeID]string,
					)

					for _, member := range placement.Replicas {
						want[member] = addresses[member]
					}

					if !reflect.DeepEqual(actual, want) {
						t.Fatalf(
							"replicas=%v expected=%v",
							actual,
							want,
						)
					}

					lookup, err := client.GetShard(
						ctx,
						&shardraftpb.GetShardRequest{
							ShardId: uint64(placement.ID),
						},
					)

					if err != nil ||
						!lookup.GetFound() ||
						lookup.GetShard().GetShardId() != uint64(placement.ID) {
						t.Fatalf(
							"shard lookup=%v err=%v",
							lookup,
							err,
						)
					}
				}

				unknown, err := client.GetShard(
					ctx,
					&shardraftpb.GetShardRequest{
						ShardId: uint64(meta.MetadataGroupID),
					},
				)

				if err != nil ||
					unknown.GetFound() {
					t.Fatalf(
						"metadata group exposed as data shard: %v %v",
						unknown,
						err,
					)
				}

				t.Logf(
					"node=%d epoch=1 shards=4 slots=65536",
					id,
				)

				t.Log("all replica addresses and shard ranges verified")
			},
		)
	}
}
