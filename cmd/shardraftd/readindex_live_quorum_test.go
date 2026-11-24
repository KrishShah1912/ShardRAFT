package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func requireLiveQuorumValue(
	t *testing.T,
	node *liveReadIndexNode,
	key string,
	expected string,
	expectedFound bool,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	value, found, err := node.shard.GetLinearizable(
		ctx,
		key,
	)

	if err != nil ||
		value != expected ||
		found != expectedFound {
		t.Fatalf(
			"key=%q value=%q found=%v err=%v; want value=%q found=%v",
			key,
			value,
			found,
			err,
			expected,
			expectedFound,
		)
	}
}

func putLiveQuorumValue(
	t *testing.T,
	c *liveReadIndexCluster,
	leader *liveReadIndexNode,
	key string,
	value string,
) {
	t.Helper()

	err := leader.shard.Propose(kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   key,
		Value: value,
	})
	if err != nil {
		t.Fatal(err)
	}

	c.waitReplicated(t, leader, key, value)
}

func TestLiveReadIndexQuorumExistingKey(t *testing.T) {
	_, leader := newLiveReadIndexLeader(t)

	requireLiveQuorumValue(
		t, leader, "alpha", "100", true,
	)

	t.Log("PASS: Existing committed key returned.")
}

func TestLiveReadIndexQuorumMissingKey(t *testing.T) {
	_, leader := newLiveReadIndexLeader(t)

	requireLiveQuorumValue(
		t, leader, "missing-key", "", false,
	)

	t.Log("PASS: Missing key returned not found.")
}

func TestLiveReadIndexQuorumOverwrite(t *testing.T) {
	c, leader := newLiveReadIndexLeader(t)

	for _, value := range []string{
		"200", "300", "400",
	} {
		putLiveQuorumValue(
			t, c, leader, "alpha", value,
		)

		requireLiveQuorumValue(
			t, leader, "alpha", value, true,
		)
	}

	t.Log("PASS: Latest committed value returned.")
}

func TestLiveReadIndexQuorumDelete(t *testing.T) {
	c, leader := newLiveReadIndexLeader(t)

	err := leader.shard.Propose(kvstore.Command{
		Op:  kvstore.OpDelete,
		Key: "alpha",
	})
	if err != nil {
		t.Fatal(err)
	}

	waitLiveReadIndexCondition(
		t,
		"delete applied on all nodes",
		func() bool {
			if err := leader.shard.Tick(); err != nil {
				t.Fatal(err)
			}

			for _, node := range c.nodes {
				value, found, err := node.shard.Get("alpha")
				if err != nil {
					t.Fatal(err)
				}

				if found || value != "" {
					return false
				}
			}

			return true
		},
	)

	requireLiveQuorumValue(
		t, leader, "alpha", "", false,
	)

	t.Log("PASS: Deleted key was not returned.")
}

func TestLiveReadIndexQuorumFollowerRejection(t *testing.T) {
	c, leader := newLiveReadIndexLeader(t)

	for id, node := range c.nodes {
		if id == leader.id {
			continue
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)

		value, found, err := node.shard.GetLinearizable(
			ctx,
			"alpha",
		)

		cancel()

		if !errors.Is(err, raft.ErrNotLeader) ||
			found ||
			value != "" {
			t.Fatalf(
				"follower %d returned value=%q found=%v err=%v",
				id,
				value,
				found,
				err,
			)
		}
	}

	t.Log("PASS: Both followers rejected linearizable reads.")
}

func TestLiveReadIndexQuorumRepeatedReads(t *testing.T) {
	_, leader := newLiveReadIndexLeader(t)

	for i := 0; i < 100; i++ {
		requireLiveQuorumValue(
			t, leader, "alpha", "100", true,
		)
	}

	t.Log("PASS: 100 sequential quorum reads completed.")
}

func runLiveConcurrentReads(
	t *testing.T,
	leader *liveReadIndexNode,
	count int,
	keyFor func(int) (string, string),
) {
	t.Helper()

	var wg sync.WaitGroup

	start := make(chan struct{})
	results := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			<-start

			key, expected := keyFor(index)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				2*time.Second,
			)
			defer cancel()

			value, found, err :=
				leader.shard.GetLinearizable(ctx, key)

			if err != nil ||
				!found ||
				value != expected {
				results <- fmt.Errorf(
					"read %d key=%q value=%q found=%v err=%v; want %q",
					index,
					key,
					value,
					found,
					err,
					expected,
				)
				return
			}

			results <- nil
		}(i)
	}

	close(start)

	wg.Wait()
	close(results)

	completed := 0

	for err := range results {
		if err != nil {
			t.Error(err)
		} else {
			completed++
		}
	}

	if completed != count {
		t.Fatalf(
			"completed %d of %d reads",
			completed,
			count,
		)
	}

	t.Logf(
		"PASS: %d concurrent reads completed.",
		completed,
	)
}

func TestLiveReadIndexQuorumConcurrentReads(t *testing.T) {
	_, leader := newLiveReadIndexLeader(t)

	runLiveConcurrentReads(
		t,
		leader,
		64,
		func(_ int) (string, string) {
			return "alpha", "100"
		},
	)
}

func TestLiveReadIndexQuorumMultipleKeys(t *testing.T) {
	c, leader := newLiveReadIndexLeader(t)

	keys := []string{
		"alpha",
		"beta",
		"gamma",
		"delta",
	}

	values := []string{
		"100",
		"200",
		"300",
		"400",
	}

	for i := 1; i < len(keys); i++ {
		putLiveQuorumValue(
			t,
			c,
			leader,
			keys[i],
			values[i],
		)
	}

	runLiveConcurrentReads(
		t,
		leader,
		64,
		func(index int) (string, string) {
			i := index % len(keys)
			return keys[i], values[i]
		},
	)
}

func TestLiveReadIndexQuorumNoLogGrowth(t *testing.T) {
	c, leader := newLiveReadIndexLeader(t)

	type nodeState struct {
		status  raft.Status
		applied uint64
	}

	before := make(map[raft.NodeID]nodeState)

	for id, node := range c.nodes {
		st, applied, err := node.shard.StatusSnapshot()
		if err != nil {
			t.Fatal(err)
		}

		before[id] = nodeState{
			status:  st,
			applied: applied,
		}
	}

	for i := 0; i < 100; i++ {
		requireLiveQuorumValue(
			t, leader, "alpha", "100", true,
		)
	}

	for id, node := range c.nodes {
		st, applied, err := node.shard.StatusSnapshot()
		if err != nil {
			t.Fatal(err)
		}

		previous := before[id]

		if st != previous.status ||
			applied != previous.applied {
			t.Fatalf(
				"node %d changed: before=%+v after=%+v applied=%d",
				id,
				previous,
				st,
				applied,
			)
		}

		t.Logf(
			"Node %d: commit=%d applied=%d unchanged",
			id,
			st.Commit,
			applied,
		)
	}

	t.Log("PASS: 100 reads caused no commit or application growth.")
}

func TestLiveReadIndexQuorumCanceledContext(t *testing.T) {
	_, leader := newLiveReadIndexLeader(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	value, found, err := leader.shard.GetLinearizable(
		ctx,
		"alpha",
	)

	if !errors.Is(err, context.Canceled) ||
		found ||
		value != "" {
		t.Fatalf(
			"unexpected canceled result: value=%q found=%v err=%v",
			value,
			found,
			err,
		)
	}

	requireLiveQuorumValue(
		t, leader, "alpha", "100", true,
	)

	t.Log("PASS: Canceled request rejected; fresh read succeeded.")
}
