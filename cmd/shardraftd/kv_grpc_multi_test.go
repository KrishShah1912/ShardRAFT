package main

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/client"
	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func multiKVTestKey(
	t *testing.T,
	id meta.ShardID,
) string {
	t.Helper()

	layout := meta.DefaultShardMap()

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("routing-key-%d", i)

		owner, err := layout.OwnerForSlot(
			meta.SlotForKey(key),
		)

		if err != nil {
			t.Fatal(err)
		}

		if owner.ID == id {
			return key
		}
	}

	t.Fatalf("no key found for shard %d", id)
	return ""
}

func multiKVTestManager(
	t *testing.T,
	id raft.NodeID,
) *shard.Manager {
	t.Helper()

	manager, err := shard.OpenManager(
		shard.ManagerConfig{
			NodeID:          id,
			DataDir:         t.TempDir(),
			ElectionTimeout: 20,
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

	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})

	return manager
}

func multiKVTestHeader(
	id meta.ShardID,
	epoch uint64,
) *shardraftpb.RequestHeader {
	return &shardraftpb.RequestHeader{
		ShardId:   uint64(id),
		Epoch:     epoch,
		ClientId:  "routing-test-client",
		RequestId: "routing-test-request",
	}
}

func TestMultiKVGRPCDeterministicRouting(t *testing.T) {
	layout := meta.DefaultShardMap()

	counts := make(map[meta.ShardID]int)

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("deterministic-key-%d", i)

		slot := meta.SlotForKey(key)

		if slot != client.HashSlot(key) {
			t.Fatalf("client/server slot mismatch: %q", key)
		}

		for repeat := 0; repeat < 3; repeat++ {
			if meta.SlotForKey(key) != slot {
				t.Fatalf("unstable slot: %q", key)
			}
		}

		owner, err := layout.OwnerForSlot(slot)

		if err != nil {
			t.Fatal(err)
		}

		expected := meta.ShardID(slot/16384 + 1)

		if owner.ID != expected {
			t.Fatalf(
				"slot=%d owner=%d expected=%d",
				slot,
				owner.ID,
				expected,
			)
		}

		counts[owner.ID]++
	}

	for id := meta.ShardID(1); id <= 4; id++ {
		if counts[id] == 0 {
			t.Fatalf("shard %d received no keys", id)
		}

		t.Logf("shard=%d keys=%d", id, counts[id])
	}

	t.Log("10000 keys: deterministic client/server routing passed")
}

func TestMultiKVGRPCLocalDispatch(t *testing.T) {
	ctx := context.Background()

	// Node 3 hosts S1, S2 and S3.
	manager := multiKVTestManager(t, 3)

	server := newMultiKVGRPCServer(
		manager,
		nil,
	)

	for _, id := range manager.GroupIDs() {
		t.Run(fmt.Sprintf("shard-%d", id), func(t *testing.T) {
			key := multiKVTestKey(t, id)

			request := &shardraftpb.GetRequest{
				Header:   multiKVTestHeader(id, 1),
				Key:      key,
				ReadMode: shardraftpb.ReadMode_READ_MODE_LOCAL,
			}

			target, failure := server.resolve(
				request,
				request.Header,
				request.Key,
				false,
			)

			if failure != nil {
				t.Fatal(failure)
			}

			group, err := manager.Group(id)

			if err != nil {
				t.Fatal(err)
			}

			if target.shardID != uint64(id) ||
				target.epoch != 1 ||
				target.node != group.Shard() {
				t.Fatal("incorrect physical shard selected")
			}

			response, err := server.Get(ctx, request)

			if err != nil {
				t.Fatal(err)
			}

			if response.GetFound() ||
				response.GetError().GetCode() !=
					shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND {
				t.Fatalf("unexpected GET response: %v", response)
			}

			put, err := server.Put(
				ctx,
				&shardraftpb.PutRequest{
					Header: multiKVTestHeader(id, 1),
					Key:    key,
					Value:  "test-value",
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			if put.GetError().GetCode() !=
				shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER {
				t.Fatalf("unexpected follower response: %v", put)
			}

			if put.GetError().GetShardId() != uint64(id) {
				t.Fatal("incorrect shard in follower response")
			}

			t.Logf(
				"key=%s routed to physical shard=%d",
				key,
				id,
			)
		})
	}
}

func TestMultiKVGRPCRejectsWrongRouting(t *testing.T) {
	ctx := context.Background()

	manager := multiKVTestManager(t, 3)

	server := newMultiKVGRPCServer(
		manager,
		nil,
	)

	before := make(map[meta.ShardID]raft.Status)

	for _, id := range manager.GroupIDs() {
		group, err := manager.Group(id)

		if err != nil {
			t.Fatal(err)
		}

		nodeStatus, err := group.Shard().Status()

		if err != nil {
			t.Fatal(err)
		}

		before[id] = nodeStatus
	}

	key := multiKVTestKey(t, 2)

	tests := []struct {
		name   string
		header *shardraftpb.RequestHeader
		key    string
		code   shardraftpb.ErrorCode
		owner  uint64
	}{
		{
			name:   "wrong-shard",
			header: multiKVTestHeader(1, 1),
			key:    key,
			code:   shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			owner:  2,
		},
		{
			name:   "wrong-epoch",
			header: multiKVTestHeader(2, 2),
			key:    key,
			code:   shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			owner:  2,
		},
		{
			name:   "non-local-shard",
			header: multiKVTestHeader(4, 1),
			key:    multiKVTestKey(t, 4),
			code:   shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			owner:  4,
		},
		{
			name:   "metadata-group",
			header: multiKVTestHeader(meta.MetadataGroupID, 1),
			key:    key,
			code:   shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			owner:  2,
		},
		{
			name: "missing-header",
			key:  key,
			code: shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		},
		{
			name:   "zero-shard",
			header: multiKVTestHeader(0, 1),
			key:    key,
			code:   shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		},
		{
			name:   "empty-key",
			header: multiKVTestHeader(2, 1),
			code:   shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			get, err := server.Get(
				ctx,
				&shardraftpb.GetRequest{
					Header: tc.header,
					Key:    tc.key,
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			put, err := server.Put(
				ctx,
				&shardraftpb.PutRequest{
					Header: tc.header,
					Key:    tc.key,
					Value:  "must-not-commit",
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			del, err := server.Delete(
				ctx,
				&shardraftpb.DeleteRequest{
					Header: tc.header,
					Key:    tc.key,
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			cas, err := server.CompareAndSwap(
				ctx,
				&shardraftpb.CompareAndSwapRequest{
					Header:   tc.header,
					Key:      tc.key,
					Expected: "old",
					Value:    "new",
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			failures := []*shardraftpb.RoutingError{
				get.GetError(),
				put.GetError(),
				del.GetError(),
				cas.GetError(),
			}

			for _, failure := range failures {
				if failure == nil ||
					failure.GetCode() != tc.code {
					t.Fatalf("unexpected failure: %v", failure)
				}

				if tc.owner != 0 &&
					failure.GetShardId() != tc.owner {
					t.Fatalf(
						"incorrect owner: %v",
						failure,
					)
				}
			}

			t.Log("GET, PUT, DELETE and CAS correctly rejected")
		})
	}

	for _, id := range manager.GroupIDs() {
		group, err := manager.Group(id)

		if err != nil {
			t.Fatal(err)
		}

		after, err := group.Shard().Status()

		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(before[id], after) {
			t.Fatalf("shard %d state changed", id)
		}
	}

	t.Log("all local Raft statuses remained unchanged")
}

func TestMultiKVGRPCUnavailable(t *testing.T) {
	ctx := context.Background()

	server := newMultiKVGRPCServer(nil, nil)

	request := &shardraftpb.GetRequest{
		Header: multiKVTestHeader(1, 1),
		Key:    multiKVTestKey(t, 1),
	}

	response, err := server.Get(ctx, request)

	if err != nil {
		t.Fatal(err)
	}

	if response.GetError().GetCode() !=
		shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE {
		t.Fatalf("unexpected response: %v", response)
	}

	response, err = server.Get(ctx, nil)

	if err != nil {
		t.Fatal(err)
	}

	if response.GetError().GetCode() !=
		shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("unexpected nil-request response: %v", response)
	}

	manager := multiKVTestManager(t, 3)

	server = newMultiKVGRPCServer(manager, nil)

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	response, err = server.Get(ctx, request)

	if err != nil {
		t.Fatal(err)
	}

	if response.GetError().GetCode() !=
		shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE {
		t.Fatalf("unexpected closed-manager response: %v", response)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = server.Get(canceled, request)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("unexpected cancellation error: %v", err)
	}

	t.Log("unavailable, closed, nil and canceled requests verified")
}
