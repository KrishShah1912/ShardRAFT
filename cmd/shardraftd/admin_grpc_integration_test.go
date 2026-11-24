package main

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type adminWireFixture struct {
	node   *shard.Shard
	server *daemonGRPCServer
	conn   *grpc.ClientConn
	client shardraftpb.AdminServiceClient
}

func newAdminWireFixture(
	t *testing.T,
) *adminWireFixture {
	t.Helper()

	node, err := shard.Open(shard.Config{
		ID:              1,
		ElectionTimeout: 5,
		WALPath:         filepath.Join(t.TempDir(), "raft.wal"),
		Send: func(messages []raft.Message) error {
			if len(messages) != 0 {
				return fmt.Errorf("unexpected network messages")
			}
			return nil
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})

	server, err := startDaemonGRPCServer(
		"127.0.0.1:0",
		node,
		nil,
	)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(server.Close)

	conn, err := grpc.NewClient(
		server.Addr().String(),
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

	return &adminWireFixture{
		node:   node,
		server: server,
		conn:   conn,
		client: shardraftpb.NewAdminServiceClient(conn),
	}
}

func adminWireContext(
	t *testing.T,
) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	t.Cleanup(cancel)

	return ctx
}

func electAdminWireLeader(
	t *testing.T,
	node *shard.Shard,
) {
	t.Helper()

	for i := 0; i < 30; i++ {
		state, err := node.Status()

		if err != nil {
			t.Fatal(err)
		}

		if state.Role == raft.Leader {
			return
		}

		if err := node.Tick(); err != nil {
			t.Fatal(err)
		}
	}

	t.Fatal("single-node shard did not elect a leader")
}

func requireAdminWireStatus(
	t *testing.T,
	f *adminWireFixture,
) *shardraftpb.StatusResponse {
	t.Helper()

	response, err := f.client.Status(
		adminWireContext(t),
		&shardraftpb.StatusRequest{
			ShardId: defaultShardID,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if response == nil {
		t.Fatal("missing status response")
	}

	if response.Id != 1 ||
		response.ShardId != defaultShardID {
		t.Fatalf("unexpected identity: %+v", response)
	}

	if response.AppliedIndex > response.CommitIndex {
		t.Fatalf("inconsistent indexes: %+v", response)
	}

	return response
}

func TestAdminGRPCWireServiceRegistration(t *testing.T) {
	f := newAdminWireFixture(t)

	services := f.server.server.GetServiceInfo()

	admin, found := services["shardraft.v1.AdminService"]

	if !found {
		t.Fatal("AdminService not registered")
	}

	methods := make(map[string]bool)

	for _, method := range admin.Methods {
		methods[method.Name] = true

		if method.IsClientStream || method.IsServerStream {
			t.Fatalf(
				"unexpected streaming method: %s",
				method.Name,
			)
		}
	}

	if len(methods) != 2 ||
		!methods["Status"] ||
		!methods["Health"] {
		t.Fatalf("unexpected admin methods: %v", methods)
	}

	if _, found := services["shardraft.v1.RaftService"]; !found {
		t.Fatal("RaftService registration missing")
	}
}

func TestAdminGRPCWireInitialStatus(t *testing.T) {
	f := newAdminWireFixture(t)

	response := requireAdminWireStatus(t, f)

	if response.Role !=
		shardraftpb.NodeRole_NODE_ROLE_FOLLOWER {
		t.Fatalf("unexpected initial role: %v", response.Role)
	}

	if response.LeaderId != 0 ||
		response.CommitIndex != 0 ||
		response.AppliedIndex != 0 {
		t.Fatalf("unexpected initial state: %+v", response)
	}
}

func TestAdminGRPCWireLeaderStatus(t *testing.T) {
	f := newAdminWireFixture(t)

	before := requireAdminWireStatus(t, f)

	electAdminWireLeader(t, f.node)

	after := requireAdminWireStatus(t, f)

	if after.Role !=
		shardraftpb.NodeRole_NODE_ROLE_LEADER {
		t.Fatalf("unexpected elected role: %v", after.Role)
	}

	if after.LeaderId != after.Id {
		t.Fatalf("unexpected leader: %d", after.LeaderId)
	}

	if after.Term <= before.Term {
		t.Fatal("term did not advance")
	}

	if after.CommitIndex == 0 ||
		after.AppliedIndex != after.CommitIndex {
		t.Fatalf("leader no-op not applied: %+v", after)
	}
}

func TestAdminGRPCWireMutationProgress(t *testing.T) {
	f := newAdminWireFixture(t)

	electAdminWireLeader(t, f.node)

	before := requireAdminWireStatus(t, f)

	const writes = 10

	for i := 0; i < writes; i++ {
		err := f.node.Propose(kvstore.Command{
			Op:        kvstore.OpPut,
			Key:       "admin-wire-key",
			Value:     fmt.Sprintf("value-%d", i),
			ClientID:  "admin-wire-client",
			RequestID: fmt.Sprintf("request-%d", i),
		})

		if err != nil {
			t.Fatal(err)
		}
	}

	after := requireAdminWireStatus(t, f)

	if after.CommitIndex != before.CommitIndex+writes ||
		after.AppliedIndex != before.AppliedIndex+writes {
		t.Fatalf("unexpected mutation progress: %+v", after)
	}

	local, applied, err := f.node.StatusSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	if after.Id != uint64(local.ID) ||
		after.Term != local.Term ||
		after.LeaderId != uint64(local.LeaderID) ||
		after.CommitIndex != local.Commit ||
		after.AppliedIndex != applied {
		t.Fatal("wire response differs from local snapshot")
	}

	value, found, err := f.node.Get("admin-wire-key")

	if err != nil || !found || value != "value-9" {
		t.Fatalf(
			"unexpected KV state: value=%q found=%v err=%v",
			value, found, err,
		)
	}
}

func TestAdminGRPCWireHealth(t *testing.T) {
	f := newAdminWireFixture(t)

	check := func() {
		t.Helper()

		response, err := f.client.Health(
			adminWireContext(t),
			&shardraftpb.HealthRequest{},
		)

		if err != nil {
			t.Fatal(err)
		}

		if !response.Healthy ||
			response.Message != "node operational" {
			t.Fatalf("unexpected health: %+v", response)
		}
	}

	// An operational follower does not need a known leader.
	check()

	electAdminWireLeader(t, f.node)

	check()
}

func TestAdminGRPCWireWrongShard(t *testing.T) {
	f := newAdminWireFixture(t)

	for _, shardID := range []uint64{
		0,
		defaultShardID + 1,
	} {
		t.Run(
			fmt.Sprintf("shard_%d", shardID),
			func(t *testing.T) {
				_, err := f.client.Status(
					adminWireContext(t),
					&shardraftpb.StatusRequest{
						ShardId: shardID,
					},
				)

				if status.Code(err) !=
					codes.FailedPrecondition {
					t.Fatalf(
						"error=%v, want FailedPrecondition",
						err,
					)
				}
			},
		)
	}
}

func TestAdminGRPCWireClosedShard(t *testing.T) {
	f := newAdminWireFixture(t)

	electAdminWireLeader(t, f.node)

	if err := f.node.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := f.client.Status(
		adminWireContext(t),
		&shardraftpb.StatusRequest{
			ShardId: defaultShardID,
		},
	)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("error=%v, want Unavailable", err)
	}

	health, err := f.client.Health(
		adminWireContext(t),
		&shardraftpb.HealthRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	if health.Healthy ||
		health.Message != "node unavailable" {
		t.Fatalf("unexpected closed-node health: %+v", health)
	}
}

func TestAdminGRPCWireCanceledContext(t *testing.T) {
	f := newAdminWireFixture(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	cancel()

	_, err := f.client.Status(
		ctx,
		&shardraftpb.StatusRequest{
			ShardId: defaultShardID,
		},
	)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("Status error=%v, want Canceled", err)
	}

	_, err = f.client.Health(
		ctx,
		&shardraftpb.HealthRequest{},
	)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("Health error=%v, want Canceled", err)
	}
}

func TestAdminGRPCWireExpiredDeadline(t *testing.T) {
	f := newAdminWireFixture(t)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)

	defer cancel()

	_, err := f.client.Status(
		ctx,
		&shardraftpb.StatusRequest{
			ShardId: defaultShardID,
		},
	)

	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf(
			"Status error=%v, want DeadlineExceeded",
			err,
		)
	}

	_, err = f.client.Health(
		ctx,
		&shardraftpb.HealthRequest{},
	)

	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf(
			"Health error=%v, want DeadlineExceeded",
			err,
		)
	}
}

func TestAdminGRPCWireConcurrentRequests(t *testing.T) {
	f := newAdminWireFixture(t)

	electAdminWireLeader(t, f.node)

	initial := requireAdminWireStatus(t, f)

	const (
		readers = 8
		reads   = 100
		writes  = 30
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)

	defer cancel()

	start := make(chan struct{})
	errs := make(chan error, readers+1)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < writes; i++ {
			err := f.node.Propose(kvstore.Command{
				Op:        kvstore.OpPut,
				Key:       "wire-concurrent",
				Value:     fmt.Sprintf("value-%d", i),
				ClientID:  "wire-concurrent-client",
				RequestID: fmt.Sprintf("request-%d", i),
			})

			if err != nil {
				errs <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for reader := 0; reader < readers; reader++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			<-start

			lastCommit := initial.CommitIndex
			lastApplied := initial.AppliedIndex

			for i := 0; i < reads; i++ {
				response, err := f.client.Status(
					ctx,
					&shardraftpb.StatusRequest{
						ShardId: defaultShardID,
					},
				)

				if err != nil {
					errs <- fmt.Errorf(
						"reader %d Status: %w",
						id, err,
					)
					return
				}

				if response.Id != initial.Id ||
					response.ShardId != defaultShardID ||
					response.Role != initial.Role ||
					response.LeaderId != initial.LeaderId ||
					response.Term != initial.Term ||
					response.AppliedIndex > response.CommitIndex ||
					response.CommitIndex < lastCommit ||
					response.AppliedIndex < lastApplied {
					errs <- fmt.Errorf(
						"reader %d inconsistent state: %+v",
						id, response,
					)
					return
				}

				lastCommit = response.CommitIndex
				lastApplied = response.AppliedIndex

				health, err := f.client.Health(
					ctx,
					&shardraftpb.HealthRequest{},
				)

				if err != nil {
					errs <- fmt.Errorf(
						"reader %d Health: %w",
						id, err,
					)
					return
				}

				if !health.Healthy {
					errs <- fmt.Errorf(
						"reader %d unhealthy node",
						id,
					)
					return
				}
			}
		}(reader)
	}

	close(start)

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	if t.Failed() {
		return
	}

	final := requireAdminWireStatus(t, f)

	if final.CommitIndex != initial.CommitIndex+writes ||
		final.AppliedIndex != final.CommitIndex {
		t.Fatalf("unexpected final state: %+v", final)
	}

	value, found, err := f.node.Get("wire-concurrent")

	if err != nil || !found || value != "value-29" {
		t.Fatalf(
			"unexpected final value=%q found=%v err=%v",
			value, found, err,
		)
	}

	t.Logf(
		"validated %d writes, %d Status RPCs and %d Health RPCs across %d readers",
		writes,
		readers*reads,
		readers*reads,
		readers,
	)
}
