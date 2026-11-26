package main

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestMetadataRuntimePlacement(t *testing.T) {
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		t.Run(fmt.Sprintf("node-%d", id), func(t *testing.T) {
			cfg := multiDaemonTestConfig(t)
			cfg.id = id

			var peers []transport.Peer

			for _, peer := range []raft.NodeID{1, 2, 3, 4, 5} {
				if peer == raft.NodeID(id) {
					continue
				}

				peers = append(peers, transport.Peer{
					ID: peer,
					Address: fmt.Sprintf(
						"127.0.0.1:%d",
						19090+peer,
					),
				})
			}

			runtime, err := openMetadataRuntime(cfg, peers)

			if err != nil {
				t.Fatal(err)
			}

			defer runtime.Close()

			expected := id <= 3

			if (runtime != nil) != expected {
				t.Fatalf(
					"node=%d expected metadata=%v",
					id,
					expected,
				)
			}

			if err := runtime.Tick(); err != nil {
				t.Fatal(err)
			}

			if err := runtime.Close(); err != nil {
				t.Fatal(err)
			}

			t.Logf(
				"node=%d metadata hosted=%v",
				id,
				expected,
			)
		})
	}
}

func TestMetadataRuntimeGRPCDispatch(t *testing.T) {
	manager := newMultiRaftTestManager(t)

	group, err := meta.OpenGroup(meta.GroupConfig{
		ID:              3,
		DataDir:         t.TempDir(),
		ElectionTimeout: 100,
		Send: func([]raft.Message) error {
			return nil
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	defer group.Close()

	before := make(map[meta.ShardID]raft.Status)

	for _, id := range manager.GroupIDs() {
		before[id] = multiRaftTestStatus(
			t, manager, id,
		)
	}

	server, err := startMultiRaftGRPCServer(
		"127.0.0.1:0",
		manager,
		3,
		group,
	)

	if err != nil {
		t.Fatal(err)
	}

	defer server.Close()

	conn, err := grpc.NewClient(
		server.Addr().String(),
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()

	client := shardraftpb.NewRaftServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)

	defer cancel()

	response, err := client.Step(
		ctx,
		multiRaftTestRequest(
			uint64(meta.MetadataGroupID),
			1,
			3,
			30,
		),
	)

	if err != nil ||
		response == nil ||
		!response.Accepted {
		t.Fatalf(
			"response=%v error=%v",
			response,
			err,
		)
	}

	state, err := group.Status()

	if err != nil {
		t.Fatal(err)
	}

	if state.Term != 30 || state.LeaderID != 1 {
		t.Fatalf(
			"incorrect metadata state: %+v",
			state,
		)
	}

	for _, id := range manager.GroupIDs() {
		after := multiRaftTestStatus(
			t, manager, id,
		)

		if !reflect.DeepEqual(before[id], after) {
			t.Fatalf(
				"metadata message changed shard %d",
				id,
			)
		}
	}

	_, err = client.Step(
		ctx,
		multiRaftTestRequest(
			uint64(meta.MetadataGroupID),
			4,
			3,
			40,
		),
	)

	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unexpected error: %v", err)
	}

	after, err := group.Status()

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(state, after) {
		t.Fatal("rejected message changed metadata state")
	}

	t.Log("metadata gRPC dispatch passed")
	t.Log("all local data groups remained unchanged")
	t.Log("invalid metadata sender rejected")
}

func TestMetadataRuntimeMissingGroup(t *testing.T) {
	manager := newMultiRaftTestManager(t)

	handler := newMultiRaftGRPCServer(
		manager,
		3,
	)

	_, err := handler.Step(
		context.Background(),
		multiRaftTestRequest(
			uint64(meta.MetadataGroupID),
			1,
			3,
			10,
		),
	)

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Log("missing local metadata group rejected")
}
