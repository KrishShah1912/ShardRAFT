package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/client"
	"github.com/KrishShah1912/shardraft/internal/meta"
)

func TestMultiShardEndToEnd(t *testing.T) {
	addresses := make(map[uint64]string)
	var listeners []net.Listener

	for id := uint64(1); id <= 5; id++ {
		listener, err := net.Listen(
			"tcp",
			"127.0.0.1:0",
		)

		if err != nil {
			for _, existing := range listeners {
				_ = existing.Close()
			}
			t.Fatal(err)
		}

		listeners = append(listeners, listener)
		addresses[id] = listener.Addr().String()
	}

	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}

	clusterCtx, stop := context.WithCancel(
		context.Background(),
	)

	type nodeResult struct {
		id  uint64
		err error
	}

	results := make(chan nodeResult, 5)
	started := 0

	t.Cleanup(func() {
		stop()

		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()

		for i := 0; i < started; i++ {
			select {
			case result := <-results:
				if result.err != nil {
					t.Errorf(
						"node %d: %v",
						result.id,
						result.err,
					)
				}
			case <-timer.C:
				t.Error("cluster shutdown timed out")
				return
			}
		}
	})

	for id := uint64(1); id <= 5; id++ {
		var peers []string
		var endpoints []string

		for peer := uint64(1); peer <= 5; peer++ {
			if peer == id {
				continue
			}

			peers = append(
				peers,
				fmt.Sprint(peer),
			)

			endpoints = append(
				endpoints,
				fmt.Sprintf(
					"%d=%s",
					peer,
					addresses[peer],
				),
			)
		}

		cfg := multiDaemonTestConfig(t)
		cfg.id = id
		cfg.peers = strings.Join(peers, ",")
		cfg.grpcPeerAddrs = strings.Join(endpoints, ",")
		cfg.grpcAddr = addresses[id]
		cfg.httpAddr = ""
		cfg.electionTimeout = 15 + int(id)*5
		cfg.tickInterval = 20 * time.Millisecond

		started++

		go func(nodeID uint64, nodeCfg config) {
			results <- nodeResult{
				id:  nodeID,
				err: runMultiRaft(clusterCtx, nodeCfg),
			}
		}(id, cfg)
	}

	t.Log("five multi-Raft storage nodes started")

	cfg := client.DefaultConfig([]string{
		addresses[1],
		addresses[2],
		addresses[3],
	})

	cfg.ClientID = "multi-shard-integration"
	cfg.MetadataTimeout = time.Second
	cfg.Retry.MaxAttempts = 10
	cfg.Retry.AttemptTimeout = 2 * time.Second

	kv, err := client.NewClient(cfg)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		40*time.Second,
	)

	defer cancel()

	deadline := time.Now().Add(10 * time.Second)
	ready := false

	for time.Now().Before(deadline) {
		err = kv.RefreshMetadata(ctx)

		if err == nil {
			ready = true
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if !ready {
		t.Fatalf("metadata discovery failed: %v", err)
	}

	t.Log("automatic metadata discovery succeeded")

	keys := make(map[meta.ShardID]string)
	values := make(map[meta.ShardID]string)

	requireValue := func(key, expected string) {
		t.Helper()

		value, found, err := kv.Get(ctx, key)

		if err != nil || !found || value != expected {
			t.Fatalf(
				"GET key=%s value=%q found=%t expected=%q err=%v",
				key,
				value,
				found,
				expected,
				err,
			)
		}
	}

	for id := meta.ShardID(1); id <= 4; id++ {
		key := multiKVTestKey(t, id)
		value := fmt.Sprintf("value-shard-%d", id)

		keys[id] = key
		values[id] = value

		result, err := kv.Put(ctx, key, value)

		if err != nil || !result.Applied {
			t.Fatalf(
				"PUT shard=%d result=%+v err=%v",
				id,
				result,
				err,
			)
		}

		requireValue(key, value)

		t.Logf(
			"shard=%d slot=%d PUT and linearizable GET passed",
			id,
			meta.SlotForKey(key),
		)
	}

	t.Log("successful KV operations across all four shards")

	identity, err := kv.NewMutationIdentity()

	if err != nil {
		t.Fatal(err)
	}

	key := keys[1]

	first, err := kv.PutWithIdentity(
		ctx,
		key,
		"deduplicated-value",
		identity,
	)

	if err != nil || !first.Applied {
		t.Fatalf(
			"initial mutation=%+v err=%v",
			first,
			err,
		)
	}

	result, err := kv.Put(
		ctx,
		key,
		"newer-value",
	)

	if err != nil || !result.Applied {
		t.Fatalf(
			"newer mutation=%+v err=%v",
			result,
			err,
		)
	}

	repeated, err := kv.PutWithIdentity(
		ctx,
		key,
		"deduplicated-value",
		identity,
	)

	if err != nil || !repeated.Applied {
		t.Fatalf(
			"duplicate mutation=%+v err=%v",
			repeated,
			err,
		)
	}

	requireValue(key, "newer-value")
	values[1] = "newer-value"

	t.Log("duplicate request did not overwrite newer value")

	result, err = kv.CompareAndSwap(
		ctx,
		keys[2],
		values[2],
		"cas-updated-value",
	)

	if err != nil || !result.Applied {
		t.Fatalf("CAS result=%+v err=%v", result, err)
	}

	values[2] = "cas-updated-value"
	requireValue(keys[2], values[2])

	t.Log("CAS succeeded on shard 2")

	result, err = kv.CompareAndSwap(
		ctx,
		keys[2],
		"incorrect-expected-value",
		"must-not-apply",
	)

	if err != nil || result.Applied {
		t.Fatalf(
			"CAS mismatch result=%+v err=%v",
			result,
			err,
		)
	}

	requireValue(keys[2], values[2])

	t.Log("CAS mismatch correctly rejected")

	result, err = kv.Delete(ctx, keys[3])

	if err != nil || !result.Applied {
		t.Fatalf(
			"DELETE result=%+v err=%v",
			result,
			err,
		)
	}

	_, found, err := kv.Get(ctx, keys[3])

	if err != nil || found {
		t.Fatalf(
			"deleted key found=%t err=%v",
			found,
			err,
		)
	}

	t.Log("DELETE succeeded on shard 3")

	for _, id := range []meta.ShardID{1, 2, 4} {
		requireValue(keys[id], values[id])

		t.Logf(
			"shard=%d independent value preserved",
			id,
		)
	}

	t.Log("cross-shard operation isolation verified")
	t.Log("multi-shard end-to-end validation passed")
}
