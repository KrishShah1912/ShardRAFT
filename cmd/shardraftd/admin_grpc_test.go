package main

import (
	"context"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type adminTestNode struct {
	state   raft.Status
	applied uint64
	err     error
	hook    func()
}

func (n *adminTestNode) StatusSnapshot() (
	raft.Status, uint64, error,
) {
	if n.hook != nil {
		n.hook()
	}

	return n.state, n.applied, n.err
}

func newAdminTestNode() *adminTestNode {
	return &adminTestNode{
		state: raft.Status{
			ID:       1,
			Role:     raft.Leader,
			Term:     5,
			LeaderID: 1,
			Commit:   12,
		},
		applied: 11,
	}
}

func requireAdminCode(
	t *testing.T,
	err error,
	want codes.Code,
) {
	t.Helper()

	if got := status.Code(err); got != want {
		t.Fatalf(
			"code=%v, want=%v, error=%v",
			got, want, err,
		)
	}
}

func TestAdminGRPCStatusSuccess(t *testing.T) {
	node := newAdminTestNode()
	server := newAdminGRPCServer(1, node)

	response, err := server.Status(
		context.Background(),
		&shardraftpb.StatusRequest{ShardId: 1},
	)

	if err != nil {
		t.Fatal(err)
	}

	if response.Id != 1 ||
		response.Role != shardraftpb.NodeRole_NODE_ROLE_LEADER ||
		response.Term != 5 ||
		response.LeaderId != 1 ||
		response.CommitIndex != 12 ||
		response.AppliedIndex != 11 ||
		response.ShardId != 1 {
		t.Fatalf("unexpected response: %v", response)
	}
}

func TestAdminGRPCStatusRoles(t *testing.T) {
	tests := []struct {
		name string
		role raft.Role
		want shardraftpb.NodeRole
	}{
		{
			"follower",
			raft.Follower,
			shardraftpb.NodeRole_NODE_ROLE_FOLLOWER,
		},
		{
			"candidate",
			raft.Candidate,
			shardraftpb.NodeRole_NODE_ROLE_CANDIDATE,
		},
		{
			"leader",
			raft.Leader,
			shardraftpb.NodeRole_NODE_ROLE_LEADER,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newAdminTestNode()
			node.state.Role = tc.role

			server := newAdminGRPCServer(1, node)

			response, err := server.Status(
				context.Background(),
				&shardraftpb.StatusRequest{ShardId: 1},
			)

			if err != nil {
				t.Fatal(err)
			}

			if response.Role != tc.want {
				t.Fatalf(
					"role=%v, want=%v",
					response.Role, tc.want,
				)
			}
		})
	}
}

func TestAdminGRPCStatusValidation(t *testing.T) {
	tests := []struct {
		name string
		req  *shardraftpb.StatusRequest
		want codes.Code
	}{
		{
			"nil_request",
			nil,
			codes.InvalidArgument,
		},
		{
			"zero_shard",
			&shardraftpb.StatusRequest{ShardId: 0},
			codes.FailedPrecondition,
		},
		{
			"wrong_shard",
			&shardraftpb.StatusRequest{ShardId: 2},
			codes.FailedPrecondition,
		},
	}

	server := newAdminGRPCServer(
		1, newAdminTestNode(),
	)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			response, err := server.Status(
				context.Background(), tc.req,
			)

			requireAdminCode(t, err, tc.want)

			if response != nil {
				t.Fatal("expected nil response")
			}
		})
	}
}

func TestAdminGRPCStatusUnavailable(t *testing.T) {
	tests := []struct {
		name string
		node adminNode
	}{
		{"nil_node", nil},
		{
			"closed_node",
			&adminTestNode{err: shard.ErrClosed},
		},
		{
			"stopped_node",
			&adminTestNode{err: shard.ErrStopped},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newAdminGRPCServer(1, tc.node)

			response, err := server.Status(
				context.Background(),
				&shardraftpb.StatusRequest{ShardId: 1},
			)

			requireAdminCode(t, err, codes.Unavailable)

			if response != nil {
				t.Fatal("expected nil response")
			}
		})
	}
}

func TestAdminGRPCStatusInconsistentState(t *testing.T) {
	tests := []struct {
		name   string
		change func(*adminTestNode)
	}{
		{
			"unknown_role",
			func(n *adminTestNode) {
				n.state.Role = raft.Role(255)
			},
		},
		{
			"zero_node_id",
			func(n *adminTestNode) {
				n.state.ID = 0
			},
		},
		{
			"applied_exceeds_commit",
			func(n *adminTestNode) {
				n.applied = n.state.Commit + 1
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newAdminTestNode()
			tc.change(node)

			server := newAdminGRPCServer(1, node)

			response, err := server.Status(
				context.Background(),
				&shardraftpb.StatusRequest{ShardId: 1},
			)

			requireAdminCode(t, err, codes.Internal)

			if response != nil {
				t.Fatal("expected nil response")
			}
		})
	}
}

func TestAdminGRPCHealthOperationalRoles(t *testing.T) {
	tests := []struct {
		name string
		role raft.Role
	}{
		{"follower", raft.Follower},
		{"candidate", raft.Candidate},
		{"leader", raft.Leader},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newAdminTestNode()
			node.state.Role = tc.role

			if tc.role != raft.Leader {
				node.state.LeaderID = 0
			}

			server := newAdminGRPCServer(1, node)

			response, err := server.Health(
				context.Background(),
				&shardraftpb.HealthRequest{},
			)

			if err != nil {
				t.Fatal(err)
			}

			if !response.Healthy {
				t.Fatalf(
					"expected healthy node: %v",
					response,
				)
			}

			if response.Message == "" {
				t.Fatal("missing health message")
			}
		})
	}
}

func TestAdminGRPCHealthUnavailable(t *testing.T) {
	tests := []struct {
		name string
		node adminNode
	}{
		{"nil_node", nil},
		{
			"closed_node",
			&adminTestNode{err: shard.ErrClosed},
		},
		{
			"stopped_node",
			&adminTestNode{err: shard.ErrStopped},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newAdminGRPCServer(1, tc.node)

			response, err := server.Health(
				context.Background(),
				&shardraftpb.HealthRequest{},
			)

			if err != nil {
				t.Fatal(err)
			}

			if response.Healthy {
				t.Fatal("unavailable node reported healthy")
			}

			if response.Message == "" {
				t.Fatal("missing health message")
			}
		})
	}
}

func TestAdminGRPCHealthInconsistentState(t *testing.T) {
	tests := []struct {
		name   string
		change func(*adminTestNode)
	}{
		{
			"unknown_role",
			func(n *adminTestNode) {
				n.state.Role = raft.Role(255)
			},
		},
		{
			"zero_node_id",
			func(n *adminTestNode) {
				n.state.ID = 0
			},
		},
		{
			"applied_exceeds_commit",
			func(n *adminTestNode) {
				n.applied = n.state.Commit + 1
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := newAdminTestNode()
			tc.change(node)

			server := newAdminGRPCServer(1, node)

			response, err := server.Health(
				context.Background(),
				&shardraftpb.HealthRequest{},
			)

			if err != nil {
				t.Fatal(err)
			}

			if response.Healthy {
				t.Fatal("inconsistent node reported healthy")
			}
		})
	}
}

func TestAdminGRPCHealthNilRequest(t *testing.T) {
	server := newAdminGRPCServer(
		1, newAdminTestNode(),
	)

	response, err := server.Health(
		context.Background(), nil,
	)

	requireAdminCode(t, err, codes.InvalidArgument)

	if response != nil {
		t.Fatal("expected nil response")
	}
}

func TestAdminGRPCCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	node := newAdminTestNode()

	node.hook = func() {
		t.Fatal("canceled request accessed node")
	}

	server := newAdminGRPCServer(1, node)

	_, err := server.Status(
		ctx,
		&shardraftpb.StatusRequest{ShardId: 1},
	)

	requireAdminCode(t, err, codes.Canceled)

	_, err = server.Health(
		ctx,
		&shardraftpb.HealthRequest{},
	)

	requireAdminCode(t, err, codes.Canceled)
}

func TestAdminGRPCExpiredDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	node := newAdminTestNode()

	node.hook = func() {
		t.Fatal("expired request accessed node")
	}

	server := newAdminGRPCServer(1, node)

	_, err := server.Status(
		ctx,
		&shardraftpb.StatusRequest{ShardId: 1},
	)

	requireAdminCode(
		t, err, codes.DeadlineExceeded,
	)

	_, err = server.Health(
		ctx,
		&shardraftpb.HealthRequest{},
	)

	requireAdminCode(
		t, err, codes.DeadlineExceeded,
	)
}

func TestAdminGRPCCancellationDuringSnapshot(t *testing.T) {
	t.Run("status", func(t *testing.T) {
		ctx, cancel := context.WithCancel(
			context.Background(),
		)
		defer cancel()

		node := newAdminTestNode()
		node.hook = cancel

		server := newAdminGRPCServer(1, node)

		response, err := server.Status(
			ctx,
			&shardraftpb.StatusRequest{ShardId: 1},
		)

		requireAdminCode(t, err, codes.Canceled)

		if response != nil {
			t.Fatal("expected nil response")
		}
	})

	t.Run("health", func(t *testing.T) {
		ctx, cancel := context.WithCancel(
			context.Background(),
		)
		defer cancel()

		node := newAdminTestNode()
		node.hook = cancel

		server := newAdminGRPCServer(1, node)

		response, err := server.Health(
			ctx,
			&shardraftpb.HealthRequest{},
		)

		requireAdminCode(t, err, codes.Canceled)

		if response != nil {
			t.Fatal("expected nil response")
		}
	})
}
