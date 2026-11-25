package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type kvDaemonNode struct {
	cfg    config
	cancel context.CancelFunc
	done   chan struct{}
	err    error
	kv     shardraftpb.KVServiceClient
	admin  shardraftpb.AdminServiceClient
	raft   shardraftpb.RaftServiceClient
}

func kvDaemonAddresses(t *testing.T, count int) []string {
	t.Helper()

	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)

	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()

	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}

	return addresses
}

func (n *kvDaemonNode) start(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	n.cancel = cancel
	n.done = make(chan struct{})
	n.err = nil

	go func() {
		defer close(n.done)
		n.err = run(ctx, n.cfg)
	}()

	t.Cleanup(func() {
		n.stop(t)
	})
}

func (n *kvDaemonNode) stop(t *testing.T) {
	t.Helper()

	n.cancel()

	select {
	case <-n.done:
		if n.err != nil {
			t.Errorf("node %d daemon error: %v", n.cfg.id, n.err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("node %d shutdown timed out", n.cfg.id)
	}
}

func newKVDaemonCluster(t *testing.T) []*kvDaemonNode {
	t.Helper()

	addresses := kvDaemonAddresses(t, 6)
	nodes := make([]*kvDaemonNode, 3)

	for i := range nodes {
		var peers []string
		var grpcPeers []string
		var httpPeers []string

		for j := range nodes {
			if i == j {
				continue
			}

			id := fmt.Sprint(j + 1)

			peers = append(peers, id)
			grpcPeers = append(grpcPeers, id+"="+addresses[j])
			httpPeers = append(httpPeers, id+"="+addresses[j+3])
		}

		node := &kvDaemonNode{
			cfg: config{
				id:              uint64(i + 1),
				peers:           strings.Join(peers, ","),
				peerAddrs:       strings.Join(httpPeers, ","),
				grpcPeerAddrs:   strings.Join(grpcPeers, ","),
				dataDir:         t.TempDir(),
				electionTimeout: 15 + i*10,
				tickInterval:    20 * time.Millisecond,
				raftTransport:   "grpc",
				grpcAddr:        addresses[i],
				httpAddr:        addresses[i+3],
			},
		}

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
			_ = conn.Close()
		})

		node.kv = shardraftpb.NewKVServiceClient(conn)
		node.admin = shardraftpb.NewAdminServiceClient(conn)
		node.raft = shardraftpb.NewRaftServiceClient(conn)

		nodes[i] = node
	}

	for _, node := range nodes {
		node.start(t)
	}

	for _, node := range nodes {
		lifecycleWaitGRPC(t, node.cfg.grpcAddr)
		lifecycleWaitHTTP(t, node.cfg.httpAddr)
	}

	return nodes
}

func kvDaemonStatus(n *kvDaemonNode) *shardraftpb.StatusResponse {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		300*time.Millisecond,
	)
	defer cancel()

	response, err := n.admin.Status(
		ctx,
		&shardraftpb.StatusRequest{
			ShardId: defaultShardID,
		},
	)

	if err != nil {
		return nil
	}

	return response
}

func kvDaemonWaitLeader(
	t *testing.T,
	nodes []*kvDaemonNode,
	excluded uint64,
) *kvDaemonNode {
	t.Helper()

	deadline := time.Now().Add(12 * time.Second)

	for time.Now().Before(deadline) {
		var leader *kvDaemonNode
		leaders := 0

		for _, node := range nodes {
			if node.cfg.id == excluded {
				continue
			}

			s := kvDaemonStatus(node)

			if s != nil &&
				s.GetRole() == shardraftpb.NodeRole_NODE_ROLE_LEADER &&
				s.GetCommitIndex() > 0 &&
				s.GetAppliedIndex() == s.GetCommitIndex() {
				leader = node
				leaders++
			}
		}

		if leaders == 1 {
			t.Logf("Production leader: node %d", leader.cfg.id)
			return leader
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("production leader election timed out")
	return nil
}

func kvDaemonHeader(requestID string) *shardraftpb.RequestHeader {
	header := &shardraftpb.RequestHeader{
		ShardId: defaultShardID,
		Epoch:   1,
	}

	if requestID != "" {
		header.ClientId = "production-daemon-test"
		header.RequestId = requestID
	}

	return header
}

func kvDaemonPut(
	t *testing.T,
	node *kvDaemonNode,
	requestID string,
	value string,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	response, err := node.kv.Put(
		ctx,
		&shardraftpb.PutRequest{
			Header: kvDaemonHeader(requestID),
			Key:    "production-key",
			Value:  value,
		},
	)

	if err != nil ||
		response == nil ||
		response.GetError() != nil ||
		!response.GetApplied() {
		t.Fatalf("put failed: response=%+v err=%v", response, err)
	}
}

func kvDaemonWaitValue(
	t *testing.T,
	node *kvDaemonNode,
	mode shardraftpb.ReadMode,
	expected string,
) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			500*time.Millisecond,
		)

		response, err := node.kv.Get(
			ctx,
			&shardraftpb.GetRequest{
				Header:   kvDaemonHeader(""),
				Key:      "production-key",
				ReadMode: mode,
			},
		)

		cancel()

		if err == nil &&
			response != nil &&
			response.GetError() == nil &&
			response.GetFound() &&
			response.GetValue() == expected {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf(
		"node %d did not return expected value %q",
		node.cfg.id,
		expected,
	)
}

func TestKVGRPCDaemonSharedListener(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	kvDaemonPut(t, leader, "put-1", "100")

	for _, node := range nodes {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		health, err := node.admin.Health(
			ctx,
			&shardraftpb.HealthRequest{},
		)

		if err != nil || !health.GetHealthy() {
			cancel()
			t.Fatalf("node %d health failed: %v", node.cfg.id, err)
		}

		_, err = node.raft.Step(
			ctx,
			&shardraftpb.StepRequest{},
		)

		cancel()

		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unexpected Raft response: %v", err)
		}

		kvDaemonWaitValue(
			t,
			node,
			shardraftpb.ReadMode_READ_MODE_LOCAL,
			"100",
		)

		t.Logf(
			"Node %d: Raft, Admin, KV and HTTP available",
			node.cfg.id,
		)
	}

	kvDaemonWaitValue(
		t,
		leader,
		shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		"100",
	)

	for _, node := range nodes {
		if node == leader {
			continue
		}

		lifecycleWait(t, func() bool {
			s := kvDaemonStatus(node)
			return s != nil && s.GetLeaderId() == leader.cfg.id
		})

		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		response, err := node.kv.Get(
			ctx,
			&shardraftpb.GetRequest{
				Header: kvDaemonHeader(""),
				Key:    "production-key",
			},
		)

		cancel()

		if err != nil || response == nil {
			t.Fatalf("follower request failed: %v", err)
		}

		hint := response.GetError()

		if hint == nil ||
			hint.GetCode() != shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER ||
			hint.GetLeaderId() != leader.cfg.id ||
			hint.GetLeaderAddress() != leader.cfg.grpcAddr ||
			response.GetFound() {
			t.Fatalf("invalid leader hint: %+v", response)
		}

		t.Logf(
			"Node %d returned configured leader address %s",
			node.cfg.id,
			hint.GetLeaderAddress(),
		)
	}

	for _, node := range nodes {
		node.stop(t)
		lifecycleAssertPortReleased(t, node.cfg.grpcAddr)
		lifecycleAssertPortReleased(t, node.cfg.httpAddr)
	}

	t.Log("PASS: Shared listeners, LOCAL reads, leader hints and shutdown")
}

func TestKVGRPCDaemonAutomaticFailover(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	kvDaemonPut(t, leader, "stable-request", "100")

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			shardraftpb.ReadMode_READ_MODE_LOCAL,
			"100",
		)
	}

	oldID := leader.cfg.id

	leader.stop(t)
	lifecycleAssertPortReleased(t, leader.cfg.grpcAddr)
	lifecycleAssertPortReleased(t, leader.cfg.httpAddr)

	t.Logf("Stopped production leader %d", oldID)

	replacement := kvDaemonWaitLeader(t, nodes, oldID)

	if replacement.cfg.id == oldID {
		t.Fatal("stopped node was selected as replacement")
	}

	kvDaemonWaitValue(
		t,
		replacement,
		shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		"100",
	)

	before := kvDaemonStatus(replacement)
	if before == nil {
		t.Fatal("missing replacement status")
	}

	kvDaemonPut(t, replacement, "stable-request", "100")

	after := kvDaemonStatus(replacement)
	if after == nil {
		t.Fatal("missing replacement status after retry")
	}

	if before.GetCommitIndex() != after.GetCommitIndex() ||
		before.GetAppliedIndex() != after.GetAppliedIndex() {
		t.Fatal("duplicate request advanced Raft indexes")
	}

	kvDaemonPut(t, replacement, "put-2", "200")

	kvDaemonWaitValue(
		t,
		replacement,
		shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
		"200",
	)

	t.Log("PASS: Automatic failover and cross-leader deduplication")
}

func TestKVGRPCDaemonMutationWithoutQuorum(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	kvDaemonPut(t, leader, "put-1", "100")

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			shardraftpb.ReadMode_READ_MODE_LOCAL,
			"100",
		)
	}

	for _, node := range nodes {
		if node != leader {
			node.stop(t)
		}
	}

	before := kvDaemonStatus(leader)
	if before == nil {
		t.Fatal("missing isolated leader status")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	response, err := leader.kv.Put(
		ctx,
		&shardraftpb.PutRequest{
			Header: kvDaemonHeader("quorum-loss-request"),
			Key:    "production-key",
			Value:  "200",
		},
	)

	cancel()

	if err != nil {
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("unexpected RPC error: %v", err)
		}
	} else {
		if response == nil ||
			response.GetApplied() ||
			response.GetError() == nil {
			t.Fatalf("mutation acknowledged without quorum: %+v", response)
		}

		code := response.GetError().GetCode()

		switch code {
		case shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
			shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE:
		default:
			t.Fatalf("unexpected mutation error: %v", code)
		}
	}

	after := kvDaemonStatus(leader)
	if after == nil {
		t.Fatal("missing isolated node status")
	}

	if before.GetCommitIndex() != after.GetCommitIndex() ||
		before.GetAppliedIndex() != after.GetAppliedIndex() {
		t.Fatal("mutation advanced indexes without quorum")
	}

	kvDaemonWaitValue(
		t,
		leader,
		shardraftpb.ReadMode_READ_MODE_LOCAL,
		"100",
	)

	t.Logf(
		"Mutation rejected without quorum: response=%+v err=%v",
		response,
		err,
	)

	t.Log("PASS: No uncommitted mutation was acknowledged or applied")
}
