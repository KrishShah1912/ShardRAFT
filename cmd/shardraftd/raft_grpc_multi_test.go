package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newMultiRaftTestManager(
	t *testing.T,
) *shard.Manager {
	t.Helper()

	manager, err := shard.OpenManager(
		shard.ManagerConfig{
			NodeID:          3,
			DataDir:         t.TempDir(),
			ElectionTimeout: 10,
			ShardMap:        meta.DefaultShardMap(),
			Send: func(
				_ meta.ShardID,
				_ []raft.Message,
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

func multiRaftTestRequest(
	groupID, from, to, term uint64,
) *shardraftpb.StepRequest {
	return &shardraftpb.StepRequest{
		ShardId: groupID,
		Message: &shardraftpb.RaftMessage{
			Type: shardraftpb.MessageType_MSG_HEARTBEAT,
			From: from,
			To:   to,
			Term: term,
		},
	}
}

func multiRaftTestStatus(
	t *testing.T,
	manager *shard.Manager,
	id meta.ShardID,
) raft.Status {
	t.Helper()

	group, err := manager.Group(id)
	if err != nil {
		t.Fatal(err)
	}

	result, err := group.Shard().Status()
	if err != nil {
		t.Fatal(err)
	}

	return result
}

func TestMultiRaftGRPCGroupDispatch(t *testing.T) {
	manager := newMultiRaftTestManager(t)
	server := newMultiRaftGRPCServer(manager, 3)

	cases := []struct {
		groupID meta.ShardID
		from    uint64
		term    uint64
	}{
		{1, 1, 11},
		{2, 2, 21},
		{3, 4, 31},
	}

	for _, tc := range cases {
		before := make(map[meta.ShardID]raft.Status)

		for _, id := range manager.GroupIDs() {
			before[id] = multiRaftTestStatus(
				t, manager, id,
			)
		}

		response, err := server.Step(
			context.Background(),
			multiRaftTestRequest(
				uint64(tc.groupID),
				tc.from,
				3,
				tc.term,
			),
		)

		if err != nil ||
			response == nil ||
			!response.Accepted {
			t.Fatalf(
				"shard=%d response=%v err=%v",
				tc.groupID,
				response,
				err,
			)
		}

		for _, id := range manager.GroupIDs() {
			after := multiRaftTestStatus(
				t, manager, id,
			)

			if id == tc.groupID {
				if after.Term != tc.term {
					t.Fatalf(
						"shard=%d term=%d want=%d",
						id,
						after.Term,
						tc.term,
					)
				}

				if after.LeaderID != raft.NodeID(tc.from) {
					t.Fatalf(
						"shard=%d leader=%d want=%d",
						id,
						after.LeaderID,
						tc.from,
					)
				}

				continue
			}

			if !reflect.DeepEqual(before[id], after) {
				t.Fatalf(
					"message for shard %d changed shard %d",
					tc.groupID,
					id,
				)
			}
		}

		t.Logf(
			"shard=%d sender=%d term=%d isolated delivery passed",
			tc.groupID,
			tc.from,
			tc.term,
		)
	}
}

func TestMultiRaftGRPCRejectsInvalidMessages(t *testing.T) {
	manager := newMultiRaftTestManager(t)
	server := newMultiRaftGRPCServer(manager, 3)

	cases := []struct {
		name    string
		request *shardraftpb.StepRequest
		code    codes.Code
	}{
		{
			"nil-request",
			nil,
			codes.InvalidArgument,
		},
		{
			"missing-message",
			&shardraftpb.StepRequest{ShardId: 1},
			codes.InvalidArgument,
		},
		{
			"zero-shard",
			multiRaftTestRequest(0, 1, 3, 100),
			codes.FailedPrecondition,
		},
		{
			"non-local-shard",
			multiRaftTestRequest(4, 1, 3, 100),
			codes.FailedPrecondition,
		},
		{
			"unknown-shard",
			multiRaftTestRequest(99, 1, 3, 100),
			codes.FailedPrecondition,
		},
		{
			"wrong-group-sender",
			multiRaftTestRequest(1, 4, 3, 100),
			codes.PermissionDenied,
		},
		{
			"wrong-destination",
			multiRaftTestRequest(1, 1, 2, 100),
			codes.InvalidArgument,
		},
		{
			"self-message",
			multiRaftTestRequest(1, 3, 3, 100),
			codes.InvalidArgument,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := make(map[meta.ShardID]raft.Status)

			for _, id := range manager.GroupIDs() {
				before[id] = multiRaftTestStatus(
					t, manager, id,
				)
			}

			_, err := server.Step(
				context.Background(),
				tc.request,
			)

			if status.Code(err) != tc.code {
				t.Fatalf(
					"code=%v want=%v err=%v",
					status.Code(err),
					tc.code,
					err,
				)
			}

			for _, id := range manager.GroupIDs() {
				after := multiRaftTestStatus(
					t, manager, id,
				)

				if !reflect.DeepEqual(before[id], after) {
					t.Fatalf(
						"rejected message changed shard %d",
						id,
					)
				}
			}
		})
	}
}

func TestMultiRaftGRPCUnavailable(t *testing.T) {
	request := multiRaftTestRequest(1, 1, 3, 10)

	server := newMultiRaftGRPCServer(nil, 3)

	_, err := server.Step(
		context.Background(),
		request,
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}

	manager := newMultiRaftTestManager(t)
	server = newMultiRaftGRPCServer(manager, 3)

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = server.Step(
		context.Background(),
		request,
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMultiRaftGRPCCanceledContext(t *testing.T) {
	manager := newMultiRaftTestManager(t)
	server := newMultiRaftGRPCServer(manager, 3)

	before := multiRaftTestStatus(t, manager, 1)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, err := server.Step(
		ctx,
		multiRaftTestRequest(1, 1, 3, 100),
	)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	after := multiRaftTestStatus(t, manager, 1)

	if !reflect.DeepEqual(before, after) {
		t.Fatal("canceled request changed shard state")
	}
}
