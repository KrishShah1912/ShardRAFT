package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	client "github.com/KrishShah1912/shardraft/internal/client"
	pb "github.com/KrishShah1912/shardraft/proto"
)

func newDistributedClient(
	t *testing.T,
	endpoints ...string,
) *client.Client {
	t.Helper()

	cfg := client.DefaultConfig(endpoints)
	cfg.MetadataTTL = time.Minute
	cfg.MetadataTimeout = time.Second
	cfg.Retry.MaxAttempts = 8
	cfg.Retry.AttemptTimeout = 2 * time.Second
	cfg.Retry.InitialBackoff = 25 * time.Millisecond
	cfg.Retry.MaxBackoff = 200 * time.Millisecond

	c, err := client.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})

	return c
}

func distributedContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)

	t.Cleanup(cancel)

	return ctx
}

func distributedRequireValue(
	t *testing.T,
	c *client.Client,
	key string,
	expected string,
) {
	t.Helper()

	value, found, err := c.Get(
		distributedContext(t),
		key,
	)

	if err != nil || !found || value != expected {
		t.Fatalf(
			"key=%q value=%q found=%v err=%v expected=%q",
			key, value, found, err, expected,
		)
	}
}

func distributedRequireMutation(
	t *testing.T,
	result client.MutationResult,
	err error,
	expected bool,
) {
	t.Helper()

	if err != nil || result.Applied != expected {
		t.Fatalf(
			"mutation=%+v err=%v expected applied=%v",
			result, err, expected,
		)
	}

	if result.Identity.ClientID == "" ||
		result.Identity.RequestID == "" {
		t.Fatal("missing mutation identity")
	}
}

func distributedFollower(
	nodes []*kvDaemonNode,
	leader *kvDaemonNode,
) *kvDaemonNode {
	for _, node := range nodes {
		if node != leader {
			return node
		}
	}

	return nil
}

func TestDistributedClientBootstrapDiscovery(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)
	follower := distributedFollower(nodes, leader)

	metadata := metaDaemonClients(t, nodes)

	metaDaemonWaitMap(
		t,
		metadata[follower.cfg.id],
		nodes,
		leader.cfg.id,
	)

	c := newDistributedClient(t, follower.cfg.grpcAddr)
	ctx := distributedContext(t)

	if err := c.RefreshMetadata(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := c.Put(ctx, "production-key", "100")
	distributedRequireMutation(t, result, err, true)

	distributedRequireValue(t, c, "production-key", "100")

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			pb.ReadMode_READ_MODE_LOCAL,
			"100",
		)
	}

	t.Logf(
		"Bootstrap through follower %d discovered leader %d",
		follower.cfg.id,
		leader.cfg.id,
	)
}

func TestDistributedClientCRUD(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	c := newDistributedClient(t, leader.cfg.grpcAddr)
	ctx := distributedContext(t)

	result, err := c.Put(ctx, "production-key", "100")
	distributedRequireMutation(t, result, err, true)

	distributedRequireValue(t, c, "production-key", "100")

	result, err = c.CompareAndSwap(
		ctx, "production-key", "100", "200",
	)
	distributedRequireMutation(t, result, err, true)

	distributedRequireValue(t, c, "production-key", "200")

	result, err = c.CompareAndSwap(
		ctx, "production-key", "100", "300",
	)
	distributedRequireMutation(t, result, err, false)

	distributedRequireValue(t, c, "production-key", "200")

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			pb.ReadMode_READ_MODE_LOCAL,
			"200",
		)
	}

	result, err = c.Delete(ctx, "production-key")
	distributedRequireMutation(t, result, err, true)

	value, found, err := c.Get(ctx, "production-key")

	if err != nil || found || value != "" {
		t.Fatalf(
			"deleted key: value=%q found=%v err=%v",
			value, found, err,
		)
	}

	t.Log("Distributed CRUD and CAS completed successfully")
}

func TestDistributedClientReadModes(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	c := newDistributedClient(t, leader.cfg.grpcAddr)
	ctx := distributedContext(t)

	result, err := c.Put(ctx, "production-key", "100")
	distributedRequireMutation(t, result, err, true)

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			pb.ReadMode_READ_MODE_LOCAL,
			"100",
		)
	}

	distributedRequireValue(t, c, "production-key", "100")

	value, found, err := c.GetLocal(ctx, "production-key")

	if err != nil || !found || value != "100" {
		t.Fatalf(
			"local read: value=%q found=%v err=%v",
			value, found, err,
		)
	}

	t.Log("Default linearizable and explicit LOCAL reads passed")
}

func TestDistributedClientLeaderFailover(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	oldLeader := kvDaemonWaitLeader(t, nodes, 0)

	// Bootstrap exclusively through the original leader.
	c := newDistributedClient(t, oldLeader.cfg.grpcAddr)
	ctx := distributedContext(t)

	result, err := c.Put(ctx, "production-key", "100")
	distributedRequireMutation(t, result, err, true)

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			pb.ReadMode_READ_MODE_LOCAL,
			"100",
		)
	}

	before := kvDaemonStatus(oldLeader)
	if before == nil {
		t.Fatal("missing original leader status")
	}

	oldLeader.stop(t)

	replacement := kvDaemonWaitLeader(
		t, nodes, oldLeader.cfg.id,
	)

	after := kvDaemonStatus(replacement)

	if after == nil || after.GetTerm() <= before.GetTerm() {
		t.Fatal("replacement leader term did not advance")
	}

	// Same client, same cached routing table.
	// No explicit metadata refresh before this read.
	distributedRequireValue(t, c, "production-key", "100")

	result, err = c.Put(
		distributedContext(t),
		"production-key",
		"200",
	)
	distributedRequireMutation(t, result, err, true)

	distributedRequireValue(t, c, "production-key", "200")

	// Refresh must also work after the only bootstrap
	// endpoint has stopped.
	if err := c.RefreshMetadata(distributedContext(t)); err != nil {
		t.Fatal(err)
	}

	t.Logf(
		"Leader failover %d -> %d; term %d -> %d",
		oldLeader.cfg.id,
		replacement.cfg.id,
		before.GetTerm(),
		after.GetTerm(),
	)
}

func TestDistributedClientBootstrapFallback(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	// Reserve a port without starting a gRPC service.
	// This guarantees the first endpoint cannot serve metadata.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	c := newDistributedClient(
		t,
		listener.Addr().String(),
		leader.cfg.grpcAddr,
	)

	ctx := distributedContext(t)

	if err := c.RefreshMetadata(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := c.Put(ctx, "production-key", "fallback")
	distributedRequireMutation(t, result, err, true)

	distributedRequireValue(
		t, c, "production-key", "fallback",
	)

	t.Log("Unavailable bootstrap endpoint bypassed successfully")
}

func TestDistributedClientDurableDeduplication(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	c := newDistributedClient(t, leader.cfg.grpcAddr)
	ctx := distributedContext(t)

	identity, err := c.NewMutationIdentity()
	if err != nil {
		t.Fatal(err)
	}

	result, err := c.PutWithIdentity(
		ctx, "production-key", "100", identity,
	)
	distributedRequireMutation(t, result, err, true)

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			pb.ReadMode_READ_MODE_LOCAL,
			"100",
		)
	}

	before := kvDaemonStatus(leader)
	if before == nil {
		t.Fatal("missing leader status")
	}

	for i := 0; i < 10; i++ {
		replay, err := c.PutWithIdentity(
			ctx, "production-key", "100", identity,
		)
		distributedRequireMutation(t, replay, err, true)

		if replay.Identity != identity {
			t.Fatal("mutation identity changed")
		}
	}

	after := kvDaemonStatus(leader)

	if after == nil ||
		before.GetCommitIndex() != after.GetCommitIndex() ||
		before.GetAppliedIndex() != after.GetAppliedIndex() {
		t.Fatal("duplicate mutations advanced Raft indexes")
	}

	leader.stop(t)

	replacement := kvDaemonWaitLeader(
		t, nodes, leader.cfg.id,
	)

	distributedRequireValue(t, c, "production-key", "100")

	before = kvDaemonStatus(replacement)
	if before == nil {
		t.Fatal("missing replacement leader status")
	}

	replay, err := c.PutWithIdentity(
		distributedContext(t),
		"production-key",
		"100",
		identity,
	)
	distributedRequireMutation(t, replay, err, true)

	if replay.Identity != identity {
		t.Fatal("cross-leader replay changed identity")
	}

	after = kvDaemonStatus(replacement)

	if after == nil ||
		before.GetCommitIndex() != after.GetCommitIndex() ||
		before.GetAppliedIndex() != after.GetAppliedIndex() {
		t.Fatal("cross-leader replay advanced Raft indexes")
	}

	t.Log("10 duplicate requests and cross-leader replay committed no extra entries")
}

func TestDistributedClientMetadataRefresh(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	c := newDistributedClient(t, leader.cfg.grpcAddr)
	ctx := distributedContext(t)

	for i := 0; i < 20; i++ {
		if err := c.RefreshMetadata(ctx); err != nil {
			t.Fatalf("refresh %d: %v", i, err)
		}
	}

	result, err := c.Put(ctx, "production-key", "100")
	distributedRequireMutation(t, result, err, true)

	distributedRequireValue(t, c, "production-key", "100")

	metadata := metaDaemonClients(t, nodes)

	for _, node := range nodes {
		result := metaDaemonWaitMap(
			t,
			metadata[node.cfg.id],
			nodes,
			leader.cfg.id,
		)

		if result.GetEpoch() != 1 ||
			result.GetTotalSlots() != client.TotalSlots ||
			len(result.GetShards()) != 1 {
			t.Fatal("unexpected production metadata")
		}
	}

	t.Log("20 explicit metadata refreshes; single-shard ownership preserved")
}

func TestDistributedClientQuorumLossAndRecovery(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	c := newDistributedClient(t, leader.cfg.grpcAddr)

	result, err := c.Put(
		distributedContext(t),
		"production-key",
		"100",
	)
	distributedRequireMutation(t, result, err, true)

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			pb.ReadMode_READ_MODE_LOCAL,
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

	readCtx, cancelRead := context.WithTimeout(
		context.Background(),
		500*time.Millisecond,
	)

	value, found, readErr := c.Get(
		readCtx, "production-key",
	)
	cancelRead()

	if readErr == nil || found || value != "" {
		t.Fatalf(
			"linearizable read succeeded without quorum: %q %v %v",
			value, found, readErr,
		)
	}

	identity, err := c.NewMutationIdentity()
	if err != nil {
		t.Fatal(err)
	}

	writeCtx, cancelWrite := context.WithTimeout(
		context.Background(),
		1500*time.Millisecond,
	)

	uncertain, writeErr := c.PutWithIdentity(
		writeCtx,
		"production-key",
		"200",
		identity,
	)

	cancelWrite()

	if writeErr == nil || uncertain.Applied {
		t.Fatal("mutation acknowledged without quorum")
	}

	if !errors.Is(writeErr, client.ErrMutationOutcomeUnknown) {
		t.Fatalf("expected uncertain mutation outcome: %v", writeErr)
	}

	if uncertain.Identity != identity {
		t.Fatal("uncertain mutation lost its identity")
	}

	after := kvDaemonStatus(leader)

	if after == nil ||
		before.GetCommitIndex() != after.GetCommitIndex() ||
		before.GetAppliedIndex() != after.GetAppliedIndex() {
		t.Fatal("mutation advanced indexes without quorum")
	}

	kvDaemonWaitValue(
		t,
		leader,
		pb.ReadMode_READ_MODE_LOCAL,
		"100",
	)

	t.Logf("Quorum-loss mutation outcome: %v", writeErr)

	// Restart stopped nodes using their original WAL directories.
	for _, node := range nodes {
		if node != leader {
			node.start(t)
			lifecycleWaitGRPC(t, node.cfg.grpcAddr)
			lifecycleWaitHTTP(t, node.cfg.httpAddr)
		}
	}

	// Wait for a leader capable of completing a linearizable read.
	deadline := time.Now().Add(20 * time.Second)
	recovered := false

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)

		value, found, err := c.Get(ctx, "production-key")
		cancel()

		if err == nil && found &&
			(value == "100" || value == "200") {
			recovered = true
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if !recovered {
		t.Fatal("linearizable reads did not recover")
	}

	// The uncertain mutation may already have committed
	// after quorum recovery. Replay the identical request.
	replay, err := c.PutWithIdentity(
		distributedContext(t),
		"production-key",
		"200",
		identity,
	)
	distributedRequireMutation(t, replay, err, true)

	if replay.Identity != identity {
		t.Fatal("recovery replay changed identity")
	}

	distributedRequireValue(t, c, "production-key", "200")

	for _, node := range nodes {
		kvDaemonWaitValue(
			t,
			node,
			pb.ReadMode_READ_MODE_LOCAL,
			"200",
		)
	}

	replacement := kvDaemonWaitLeader(t, nodes, 0)

	before = kvDaemonStatus(replacement)
	if before == nil {
		t.Fatal("missing recovered leader status")
	}

	replay, err = c.PutWithIdentity(
		distributedContext(t),
		"production-key",
		"200",
		identity,
	)
	distributedRequireMutation(t, replay, err, true)

	after = kvDaemonStatus(replacement)

	if after == nil ||
		before.GetCommitIndex() != after.GetCommitIndex() ||
		before.GetAppliedIndex() != after.GetAppliedIndex() {
		t.Fatal("recovery replay advanced Raft indexes")
	}

	t.Log("Quorum restored; uncertain mutation replayed with original identity")
}

func TestDistributedClientConcurrentOperations(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	c := newDistributedClient(t, leader.cfg.grpcAddr)
	ctx := distributedContext(t)

	const workers = 8
	const operations = 10

	var wg sync.WaitGroup
	var mu sync.Mutex

	identities := make(map[client.MutationIdentity]bool)
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()

			for j := 0; j < operations; j++ {
				key := fmt.Sprintf(
					"concurrent-%d-%d", worker, j,
				)

				expected := fmt.Sprintf("value-%d", j)

				result, err := c.Put(ctx, key, expected)

				if err != nil || !result.Applied {
					errs <- fmt.Errorf(
						"put %s: %+v %v",
						key, result, err,
					)
					return
				}

				mu.Lock()
				duplicate := identities[result.Identity]
				identities[result.Identity] = true
				mu.Unlock()

				if duplicate {
					errs <- fmt.Errorf(
						"duplicate identity for %s", key,
					)
					return
				}

				value, found, err := c.Get(ctx, key)

				if err != nil || !found || value != expected {
					errs <- fmt.Errorf(
						"get %s: %q %v %v",
						key, value, found, err,
					)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	if t.Failed() {
		return
	}

	if len(identities) != workers*operations {
		t.Fatalf("unexpected identity count: %d", len(identities))
	}

	t.Logf(
		"Validated %d concurrent writes and %d linearizable reads",
		workers*operations,
		workers*operations,
	)
}

func TestDistributedClientDeadline(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	c := newDistributedClient(t, leader.cfg.grpcAddr)

	result, err := c.Put(
		distributedContext(t),
		"production-key",
		"100",
	)
	distributedRequireMutation(t, result, err, true)

	for _, node := range nodes {
		if node != leader {
			node.stop(t)
		}
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)
	defer cancel()

	start := time.Now()

	value, found, err := c.Get(ctx, "production-key")
	elapsed := time.Since(start)

	if err == nil || found || value != "" {
		t.Fatal("read succeeded without quorum")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected deadline error: %v", err)
	}

	if elapsed > 2*time.Second {
		t.Fatalf("deadline enforcement too slow: %v", elapsed)
	}

	t.Logf("Read deadline enforced in %v", elapsed)
}

func TestDistributedClientUnavailableBeforeDispatch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	c := newDistributedClient(t, listener.Addr().String())

	ctx, cancel := context.WithTimeout(
		context.Background(),
		300*time.Millisecond,
	)
	defer cancel()

	result, err := c.Put(ctx, "production-key", "100")

	if err == nil || result.Applied {
		t.Fatal("mutation unexpectedly succeeded")
	}

	if errors.Is(err, client.ErrMutationOutcomeUnknown) {
		t.Fatal("metadata failure incorrectly marked mutation ambiguous")
	}

	if result.Identity.RequestID == "" {
		t.Fatal("mutation identity was lost")
	}

	t.Logf("Pre-dispatch failure correctly classified: %v", err)
}

func TestDistributedClientClose(t *testing.T) {
	nodes := newKVDaemonCluster(t)
	leader := kvDaemonWaitLeader(t, nodes, 0)

	c := newDistributedClient(t, leader.cfg.grpcAddr)
	ctx := distributedContext(t)

	result, err := c.Put(ctx, "production-key", "100")
	distributedRequireMutation(t, result, err, true)

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, err = c.Get(ctx, "production-key")

	if !errors.Is(err, client.ErrConnectionManagerClosed) {
		t.Fatalf("unexpected closed-client error: %v", err)
	}

	for _, node := range nodes {
		node.stop(t)
		lifecycleAssertPortReleased(t, node.cfg.grpcAddr)
		lifecycleAssertPortReleased(t, node.cfg.httpAddr)
	}

	t.Log("Client closure and daemon listener cleanup passed")
}
