package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type kvLiveCluster struct {
	cluster   *liveReadIndexCluster
	leader    *liveReadIndexNode
	clients   map[raft.NodeID]shardraftpb.KVServiceClient
	addresses map[raft.NodeID]string
}

func newKVLiveCluster(t *testing.T) *kvLiveCluster {
	t.Helper()

	c := newLiveReadIndexCluster(t)

	f := &kvLiveCluster{
		cluster:   c,
		clients:   make(map[raft.NodeID]shardraftpb.KVServiceClient),
		addresses: make(map[raft.NodeID]string),
	}

	listeners := make(map[raft.NodeID]net.Listener)

	for id := raft.NodeID(1); id <= 3; id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		listeners[id] = listener
		f.addresses[id] = listener.Addr().String()

		t.Cleanup(func() {
			_ = listener.Close()
		})
	}

	for id := raft.NodeID(1); id <= 3; id++ {
		node := c.nodes[id]
		listener := listeners[id]

		server := grpc.NewServer()

		shardraftpb.RegisterKVServiceServer(
			server,
			newKVGRPCServer(
				defaultShardID,
				node.shard,
				f.addresses,
			),
		)

		shardraftpb.RegisterAdminServiceServer(
			server,
			newAdminGRPCServer(defaultShardID, node.shard),
		)

		done := make(chan struct{})

		go func() {
			defer close(done)
			_ = server.Serve(listener)
		}()

		t.Cleanup(func() {
			server.Stop()
			<-done
		})

		conn, err := grpc.NewClient(
			f.addresses[id],
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)

		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			_ = conn.Close()
		})

		f.clients[id] = shardraftpb.NewKVServiceClient(conn)
	}

	f.leader = c.elect(t, 1)

	return f
}

func kvLiveHeader(requestID string) *shardraftpb.RequestHeader {
	header := &shardraftpb.RequestHeader{
		ShardId: defaultShardID,
		Epoch:   1,
	}

	if requestID != "" {
		header.ClientId = "cluster-test-client"
		header.RequestId = requestID
	}

	return header
}

func kvLivePut(
	t *testing.T,
	f *kvLiveCluster,
	requestID string,
	key string,
	value string,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	response, err := f.clients[f.leader.id].Put(
		ctx,
		&shardraftpb.PutRequest{
			Header: kvLiveHeader(requestID),
			Key:    key,
			Value:  value,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if response == nil ||
		response.GetError().GetCode() != 0 ||
		!response.GetApplied() {
		t.Fatalf("unexpected put response: %+v", response)
	}

	f.cluster.waitReplicated(t, f.leader, key, value)
}

func kvLiveGet(
	t *testing.T,
	f *kvLiveCluster,
	id raft.NodeID,
	key string,
	expected string,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	response, err := f.clients[id].Get(
		ctx,
		&shardraftpb.GetRequest{
			Header: kvLiveHeader(""),
			Key:    key,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if response == nil ||
		response.GetError().GetCode() != 0 ||
		!response.GetFound() ||
		response.GetValue() != expected {
		t.Fatalf(
			"node=%d response=%+v expected=%q",
			id,
			response,
			expected,
		)
	}
}

func TestKVGRPCClusterReplication(t *testing.T) {
	f := newKVLiveCluster(t)

	kvLivePut(t, f, "put-1", "alpha", "100")
	kvLiveGet(t, f, f.leader.id, "alpha", "100")

	kvLivePut(t, f, "put-2", "alpha", "200")
	kvLiveGet(t, f, f.leader.id, "alpha", "200")

	t.Log("PASS: gRPC writes replicated across all three nodes.")
}

func TestKVGRPCClusterFollowerHints(t *testing.T) {
	f := newKVLiveCluster(t)

	kvLivePut(t, f, "put-1", "alpha", "100")

	for _, id := range []raft.NodeID{2, 3} {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		response, err := f.clients[id].Get(
			ctx,
			&shardraftpb.GetRequest{
				Header: kvLiveHeader(""),
				Key:    "alpha",
			},
		)

		cancel()

		if err != nil {
			t.Fatal(err)
		}

		routing := response.GetError()

		if routing == nil ||
			routing.GetCode() != 1 ||
			routing.GetLeaderId() != uint64(f.leader.id) ||
			routing.GetLeaderAddress() != f.addresses[f.leader.id] ||
			response.GetFound() {
			t.Fatalf(
				"node=%d unexpected response: %+v",
				id,
				response,
			)
		}

		t.Logf(
			"Follower %d returned leader %d at %s",
			id,
			routing.GetLeaderId(),
			routing.GetLeaderAddress(),
		)
	}
}

func TestKVGRPCClusterLeaderReplacement(t *testing.T) {
	f := newKVLiveCluster(t)

	kvLivePut(t, f, "put-1", "alpha", "100")

	previous := f.leader

	f.leader = replaceLiveReadIndexLeader(
		t,
		f.cluster,
		previous,
		2,
	)

	kvLiveGet(t, f, f.leader.id, "alpha", "100")

	kvLivePut(t, f, "put-2", "alpha", "200")
	kvLiveGet(t, f, f.leader.id, "alpha", "200")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	response, err := f.clients[previous.id].Get(
		ctx,
		&shardraftpb.GetRequest{
			Header: kvLiveHeader(""),
			Key:    "alpha",
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	routing := response.GetError()

	if routing == nil ||
		routing.GetCode() != 1 ||
		routing.GetLeaderId() != uint64(f.leader.id) ||
		routing.GetLeaderAddress() != f.addresses[f.leader.id] ||
		response.GetFound() {
		t.Fatalf("former leader response: %+v", response)
	}

	t.Log("PASS: Replacement leader served latest data.")
}

func TestKVGRPCClusterDedupAfterLeadershipChange(t *testing.T) {
	f := newKVLiveCluster(t)

	kvLivePut(t, f, "stable-request", "alpha", "100")

	f.leader = replaceLiveReadIndexLeader(
		t,
		f.cluster,
		f.leader,
		2,
	)

	before, beforeApplied, err := f.leader.shard.StatusSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	kvLivePut(t, f, "stable-request", "alpha", "100")

	after, afterApplied, err := f.leader.shard.StatusSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	if before.Commit != after.Commit ||
		beforeApplied != afterApplied {
		t.Fatalf(
			"duplicate request changed indexes: commit=%d->%d applied=%d->%d",
			before.Commit,
			after.Commit,
			beforeApplied,
			afterApplied,
		)
	}

	kvLiveGet(t, f, f.leader.id, "alpha", "100")

	t.Log("PASS: Deduplication survived leadership replacement.")
}

func TestKVGRPCClusterOneFollowerUnavailable(t *testing.T) {
	f := newKVLiveCluster(t)

	kvLivePut(t, f, "put-1", "alpha", "100")

	stopLiveReadIndexInbound(t, f.cluster.nodes[3])

	for i := 0; i < 20; i++ {
		kvLiveGet(t, f, f.leader.id, "alpha", "100")
	}

	t.Log("PASS: Linearizable reads remained available with one follower down.")
}

func TestKVGRPCClusterQuorumLossAndRecovery(t *testing.T) {
	f := newKVLiveCluster(t)

	kvLivePut(t, f, "put-1", "alpha", "100")

	address2, address3 := isolateLiveReadIndexFollowers(
		t,
		f.cluster,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)

	response, err := f.clients[f.leader.id].Get(
		ctx,
		&shardraftpb.GetRequest{
			Header: kvLiveHeader(""),
			Key:    "alpha",
		},
	)

	cancel()

	if err != nil {
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("unexpected quorum-loss RPC error: %v", err)
		}
	} else {
		if response == nil ||
			response.GetError() == nil ||
			response.GetError().GetCode() != shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE ||
			response.GetFound() ||
			response.GetValue() != "" {
			t.Fatalf("unexpected quorum-loss response: %+v", response)
		}
	}

	t.Logf(
		"Read rejected during quorum loss: response=%+v err=%v",
		response,
		err,
	)

	restoreLiveReadIndexInbound(
		t,
		f.cluster.nodes[2],
		address2,
	)

	waitLiveReadIndexRecovery(t, f.leader, "100")

	kvLiveGet(t, f, f.leader.id, "alpha", "100")

	restoreLiveReadIndexInbound(
		t,
		f.cluster.nodes[3],
		address3,
	)

	kvLivePut(t, f, "put-2", "alpha", "200")
	kvLiveGet(t, f, f.leader.id, "alpha", "200")

	t.Log("PASS: KV service recovered after quorum restoration.")
}
