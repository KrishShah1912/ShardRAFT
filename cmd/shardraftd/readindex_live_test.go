package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	"github.com/KrishShah1912/shardraft/internal/transport"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
)

type liveReadIndexNode struct {
	id        raft.NodeID
	shard     *shard.Shard
	transport *transport.GRPCTransport
	server    *grpc.Server
	listener  net.Listener
	done      chan struct{}
}

type liveReadIndexCluster struct {
	nodes map[raft.NodeID]*liveReadIndexNode
}

func newLiveReadIndexCluster(t *testing.T) *liveReadIndexCluster {
	t.Helper()

	c := &liveReadIndexCluster{
		nodes: make(map[raft.NodeID]*liveReadIndexNode),
	}

	root := t.TempDir()

	t.Cleanup(func() {
		for _, n := range c.nodes {
			if n.server != nil {
				n.server.Stop()
			}

			if n.listener != nil {
				_ = n.listener.Close()
			}
		}

		for _, n := range c.nodes {
			if n.done != nil {
				<-n.done
			}
		}

		for _, n := range c.nodes {
			if n.transport != nil {
				if err := n.transport.Close(); err != nil {
					t.Errorf("close transport %d: %v", n.id, err)
				}
			}
		}

		for _, n := range c.nodes {
			if n.shard != nil {
				if err := n.shard.Close(); err != nil {
					t.Errorf("close shard %d: %v", n.id, err)
				}
			}
		}
	})

	for id := raft.NodeID(1); id <= 3; id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		c.nodes[id] = &liveReadIndexNode{
			id:       id,
			listener: listener,
		}

		t.Logf(
			"Node %d listening on %s",
			id,
			listener.Addr(),
		)
	}

	for id := raft.NodeID(1); id <= 3; id++ {
		n := c.nodes[id]

		var peerIDs []raft.NodeID
		var peers []transport.Peer

		for peerID := raft.NodeID(1); peerID <= 3; peerID++ {
			if peerID == id {
				continue
			}

			peerIDs = append(peerIDs, peerID)

			peers = append(peers, transport.Peer{
				ID:      peerID,
				Address: c.nodes[peerID].listener.Addr().String(),
			})
		}

		tr, err := transport.NewGRPCTransport(
			1,
			peers,
			4096,
		)
		if err != nil {
			t.Fatal(err)
		}

		n.transport = tr

		s, err := shard.Open(shard.Config{
			ID:              id,
			Peers:           peerIDs,
			ElectionTimeout: 10 + int(id)*10,
			WALPath: filepath.Join(
				root,
				fmt.Sprintf("node%d.wal", id),
			),
			Send: tr.Send,
		})
		if err != nil {
			t.Fatal(err)
		}

		n.shard = s

		n.server = grpc.NewServer()

		shardraftpb.RegisterRaftServiceServer(
			n.server,
			newRaftGRPCServer(1, s, peerIDs),
		)

		n.done = make(chan struct{})

		go func(n *liveReadIndexNode) {
			defer close(n.done)
			_ = n.server.Serve(n.listener)
		}(n)
	}

	return c
}

func waitLiveReadIndexCondition(
	t *testing.T,
	description string,
	condition func() bool,
) {
	t.Helper()

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}

		select {
		case <-timer.C:
			t.Fatalf("timed out: %s", description)
		case <-ticker.C:
		}
	}
}

func (c *liveReadIndexCluster) elect(
	t *testing.T,
	id raft.NodeID,
) *liveReadIndexNode {
	t.Helper()

	n := c.nodes[id]

	waitLiveReadIndexCondition(
		t,
		"leader election and committed no-op",
		func() bool {
			if err := n.shard.Tick(); err != nil {
				t.Fatal(err)
			}

			st, applied, err := n.shard.StatusSnapshot()
			if err != nil {
				t.Fatal(err)
			}

			if st.Role != raft.Leader ||
				st.Term == 0 ||
				st.Commit == 0 ||
				applied < st.Commit {
				return false
			}

			// Leadership alone does not establish ReadIndex
			// readiness. A current-term entry must be committed.
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second,
			)

			_, _, err = n.shard.GetLinearizable(
				ctx,
				"__readindex_readiness_probe__",
			)

			cancel()

			if errors.Is(err, raft.ErrReadNotReady) {
				return false
			}

			if err != nil {
				t.Fatalf(
					"leader readiness probe failed: %v",
					err,
				)
			}

			return true
		},
	)

	st, err := n.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf(
		"Node %d elected leader: term=%d commit=%d",
		id,
		st.Term,
		st.Commit,
	)

	return n
}

func (c *liveReadIndexCluster) waitReplicated(
	t *testing.T,
	leader *liveReadIndexNode,
	key string,
	expected string,
) {
	t.Helper()

	waitLiveReadIndexCondition(
		t,
		"committed value on all three nodes",
		func() bool {
			if err := leader.shard.Tick(); err != nil {
				t.Fatal(err)
			}

			for _, n := range c.nodes {
				value, found, err := n.shard.Get(key)
				if err != nil {
					t.Fatal(err)
				}

				if !found || value != expected {
					return false
				}
			}

			return true
		},
	)

	for id := raft.NodeID(1); id <= 3; id++ {
		n := c.nodes[id]

		st, applied, err := n.shard.StatusSnapshot()
		if err != nil {
			t.Fatal(err)
		}

		t.Logf(
			"Node %d: term=%d commit=%d applied=%d value=%s",
			id,
			st.Term,
			st.Commit,
			applied,
			expected,
		)
	}
}

func newLiveReadIndexLeader(
	t *testing.T,
) (*liveReadIndexCluster, *liveReadIndexNode) {
	t.Helper()

	c := newLiveReadIndexCluster(t)
	leader := c.elect(t, 1)

	err := leader.shard.Propose(kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   "alpha",
		Value: "100",
	})
	if err != nil {
		t.Fatal(err)
	}

	c.waitReplicated(t, leader, "alpha", "100")

	return c, leader
}

func TestLiveReadIndexHarnessElection(t *testing.T) {
	c := newLiveReadIndexCluster(t)
	leader := c.elect(t, 1)

	st, err := leader.shard.Status()
	if err != nil {
		t.Fatal(err)
	}

	for id, n := range c.nodes {
		if id == leader.id {
			continue
		}

		waitLiveReadIndexCondition(
			t,
			"follower recognizes elected leader",
			func() bool {
				if err := leader.shard.Tick(); err != nil {
					t.Fatal(err)
				}

				follower, err := n.shard.Status()
				if err != nil {
					t.Fatal(err)
				}

				return follower.Role == raft.Follower &&
					follower.Term == st.Term &&
					follower.LeaderID == leader.id
			},
		)
	}

	t.Log("PASS: Real gRPC three-node election.")
}

func TestLiveReadIndexHarnessReplication(t *testing.T) {
	c, leader := newLiveReadIndexLeader(t)

	if err := leader.shard.Propose(kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   "alpha",
		Value: "200",
	}); err != nil {
		t.Fatal(err)
	}

	c.waitReplicated(t, leader, "alpha", "200")

	t.Log("PASS: Real gRPC replication and application.")
}

func TestLiveReadIndexHarnessTransport(t *testing.T) {
	c, _ := newLiveReadIndexLeader(t)

	for id, n := range c.nodes {
		waitLiveReadIndexCondition(
			t,
			fmt.Sprintf("node %d transport delivery", id),
			func() bool {
				return n.transport.Stats().Delivered > 0
			},
		)

		stats := n.transport.Stats()

		if stats.Closed || stats.Failures != 0 {
			t.Fatalf(
				"unexpected node %d transport stats: %+v",
				id,
				stats,
			)
		}

		t.Logf(
			"Node %d: sent=%d delivered=%d failures=%d",
			id,
			stats.Sent,
			stats.Delivered,
			stats.Failures,
		)
	}

	t.Log("PASS: Production gRPC transport delivered Raft messages.")
}

func TestLiveReadIndexHarnessRead(t *testing.T) {
	_, leader := newLiveReadIndexLeader(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	value, found, err := leader.shard.GetLinearizable(
		ctx,
		"alpha",
	)

	if err != nil || !found || value != "100" {
		t.Fatalf(
			"unexpected linearizable read: value=%q found=%v err=%v",
			value,
			found,
			err,
		)
	}

	t.Logf(
		"PASS: First real gRPC quorum read: alpha=%s",
		value,
	)
}
