package shard

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

type managerTestMessage struct {
	groupID meta.ShardID
	message raft.Message
}

type managerTestCluster struct {
	nodes   map[raft.NodeID]*Manager
	configs map[raft.NodeID]ManagerConfig
	queue   []managerTestMessage
	layout  meta.ShardMap
}

func newManagerTestCluster(
	t *testing.T,
) *managerTestCluster {
	t.Helper()

	cluster := &managerTestCluster{
		nodes:   make(map[raft.NodeID]*Manager),
		configs: make(map[raft.NodeID]ManagerConfig),
		layout:  meta.DefaultShardMap(),
	}

	root := t.TempDir()

	t.Cleanup(func() {
		for id, manager := range cluster.nodes {
			if err := manager.Close(); err != nil {
				t.Errorf("close node %d: %v", id, err)
			}
		}
	})

	for id := raft.NodeID(1); id <= 5; id++ {
		cfg := ManagerConfig{
			NodeID: id,
			DataDir: filepath.Join(
				root,
				fmt.Sprintf("node-%d", id),
			),
			ElectionTimeout: 10 + int(id)*10,
			ShardMap:        cluster.layout,
			Send: func(
				groupID meta.ShardID,
				messages []raft.Message,
			) error {
				for _, message := range messages {
					message.Entries = append(
						[]raft.Entry(nil),
						message.Entries...,
					)

					for i := range message.Entries {
						message.Entries[i].Data = append(
							[]byte(nil),
							message.Entries[i].Data...,
						)
					}

					cluster.queue = append(
						cluster.queue,
						managerTestMessage{
							groupID: groupID,
							message: message,
						},
					)
				}

				return nil
			},
		}

		manager, err := OpenManager(cfg)
		if err != nil {
			t.Fatalf("open node %d: %v", id, err)
		}

		cluster.nodes[id] = manager
		cluster.configs[id] = cfg
	}

	return cluster
}

func (c *managerTestCluster) deliver(
	t *testing.T,
) {
	t.Helper()

	for round := 0; round < 1000; round++ {
		if len(c.queue) == 0 {
			return
		}

		messages := c.queue
		c.queue = nil

		for _, item := range messages {
			target, ok := c.nodes[item.message.To]
			if !ok {
				t.Fatalf(
					"unknown destination node %d",
					item.message.To,
				)
			}

			source, ok := c.nodes[item.message.From]
			if !ok {
				t.Fatalf(
					"unknown source node %d",
					item.message.From,
				)
			}

			if _, err := source.Group(
				item.groupID,
			); err != nil {
				t.Fatalf(
					"source does not host shard %d: %v",
					item.groupID,
					err,
				)
			}

			if err := target.Step(
				item.groupID,
				item.message,
			); err != nil {
				t.Fatalf(
					"deliver shard=%d from=%d to=%d: %v",
					item.groupID,
					item.message.From,
					item.message.To,
					err,
				)
			}
		}
	}

	t.Fatal("multi-group network did not become idle")
}

func (c *managerTestCluster) tick(
	t *testing.T,
) {
	t.Helper()

	for id := raft.NodeID(1); id <= 5; id++ {
		manager := c.nodes[id]

		for _, groupID := range manager.GroupIDs() {
			if err := manager.Tick(groupID); err != nil {
				t.Fatalf(
					"tick node=%d shard=%d: %v",
					id,
					groupID,
					err,
				)
			}
		}
	}

	c.deliver(t)
}

func (c *managerTestCluster) group(
	t *testing.T,
	nodeID raft.NodeID,
	groupID meta.ShardID,
) *Group {
	t.Helper()

	group, err := c.nodes[nodeID].Group(groupID)
	if err != nil {
		t.Fatal(err)
	}

	return group
}

func (c *managerTestCluster) leaders(
	t *testing.T,
) map[meta.ShardID]raft.NodeID {
	t.Helper()

	leaders := make(
		map[meta.ShardID]raft.NodeID,
	)

	for _, placement := range c.layout.Shards {
		for _, nodeID := range placement.Replicas {
			group := c.group(
				t, nodeID, placement.ID,
			)

			status, err := group.Shard().Status()
			if err != nil {
				t.Fatal(err)
			}

			if status.Role != raft.Leader {
				continue
			}

			if previous := leaders[placement.ID]; previous != 0 {
				t.Fatalf(
					"multiple leaders for shard %d: %d and %d",
					placement.ID,
					previous,
					nodeID,
				)
			}

			leaders[placement.ID] = nodeID
		}
	}

	return leaders
}

func (c *managerTestCluster) elect(
	t *testing.T,
) map[meta.ShardID]raft.NodeID {
	t.Helper()

	for i := 0; i < 300; i++ {
		c.tick(t)

		leaders := c.leaders(t)

		if len(leaders) != len(c.layout.Shards) {
			continue
		}

		ready := true

		for _, placement := range c.layout.Shards {
			leaderID := leaders[placement.ID]

			for _, nodeID := range placement.Replicas {
				group := c.group(
					t, nodeID, placement.ID,
				)

				status, applied, err :=
					group.Shard().StatusSnapshot()

				if err != nil {
					t.Fatal(err)
				}

				if status.LeaderID != leaderID ||
					status.Commit == 0 ||
					applied != status.Commit {
					ready = false
				}
			}
		}

		if ready {
			for _, placement := range c.layout.Shards {
				t.Logf(
					"shard=%d leader=%d replicas=%v",
					placement.ID,
					leaders[placement.ID],
					placement.Replicas,
				)
			}

			return leaders
		}
	}

	t.Fatal("not all shard groups elected leaders")
	return nil
}

func (c *managerTestCluster) requireValue(
	t *testing.T,
	groupID meta.ShardID,
	key, expected string,
) {
	t.Helper()

	for _, placement := range c.layout.Shards {
		if placement.ID != groupID {
			continue
		}

		for _, nodeID := range placement.Replicas {
			group := c.group(
				t, nodeID, groupID,
			)

			value, found, err := group.Get(key)

			if err != nil ||
				!found ||
				value != expected {
				t.Fatalf(
					"node=%d shard=%d key=%q value=%q found=%v err=%v",
					nodeID,
					groupID,
					key,
					value,
					found,
					err,
				)
			}
		}

		return
	}

	t.Fatalf("unknown shard %d", groupID)
}

func (c *managerTestCluster) write(
	t *testing.T,
	groupID meta.ShardID,
	leaderID raft.NodeID,
	command kvstore.Command,
) {
	t.Helper()

	group := c.group(t, leaderID, groupID)

	routed, err := c.nodes[leaderID].GroupForKey(
		command.Key,
	)
	if err != nil {
		t.Fatal(err)
	}

	if routed != group {
		t.Fatal("request routed to incorrect local group")
	}

	if err := routed.Propose(command); err != nil {
		t.Fatal(err)
	}

	c.deliver(t)

	for i := 0; i < 10; i++ {
		c.tick(t)
	}

	c.requireValue(
		t,
		groupID,
		command.Key,
		command.Value,
	)
}

func (c *managerTestCluster) requireIsolation(
	t *testing.T,
	keys map[meta.ShardID]string,
) {
	t.Helper()

	for _, placement := range c.layout.Shards {
		for _, nodeID := range placement.Replicas {
			group := c.group(
				t, nodeID, placement.ID,
			)

			for owner, key := range keys {
				if owner == placement.ID {
					continue
				}

				_, found, err :=
					group.Shard().Get(key)

				if err != nil {
					t.Fatal(err)
				}

				if found {
					t.Fatalf(
						"key %q leaked from shard %d into shard %d on node %d",
						key,
						owner,
						placement.ID,
						nodeID,
					)
				}

				var wrong *WrongShardError

				if _, _, err := group.Get(key); !errors.As(
					err, &wrong,
				) {
					t.Fatalf(
						"foreign key was not rejected: %v",
						err,
					)
				}

				if wrong.Owner != owner {
					t.Fatalf(
						"incorrect owner: %d want=%d",
						wrong.Owner,
						owner,
					)
				}
			}
		}
	}
}

func TestManagerMultiGroupReplication(
	t *testing.T,
) {
	cluster := newManagerTestCluster(t)

	leaders := cluster.elect(t)

	keys := make(map[meta.ShardID]string)

	for _, placement := range cluster.layout.Shards {
		groupID := placement.ID

		key := keyForShard(
			t, cluster.layout, groupID,
		)

		keys[groupID] = key

		value := fmt.Sprintf(
			"value-for-shard-%d", groupID,
		)

		cluster.write(
			t,
			groupID,
			leaders[groupID],
			kvstore.Command{
				Op:       kvstore.OpPut,
				Key:      key,
				Value:    value,
				ClientID: "multi-group-client",
				RequestID: fmt.Sprintf(
					"put-%d", groupID,
				),
			},
		)

		t.Logf(
			"shard=%d key=%q replicated to all three replicas",
			groupID,
			key,
		)
	}

	cluster.requireIsolation(t, keys)

	t.Log("all 12 replicas verified")
	t.Log("no cross-shard data leakage")
}

func TestManagerMultiGroupRecovery(
	t *testing.T,
) {
	cluster := newManagerTestCluster(t)

	leaders := cluster.elect(t)

	keys := make(map[meta.ShardID]string)
	values := make(map[meta.ShardID]string)
	commands := make(
		map[meta.ShardID]kvstore.Command,
	)

	for _, placement := range cluster.layout.Shards {
		id := placement.ID

		keys[id] = keyForShard(
			t, cluster.layout, id,
		)

		values[id] = fmt.Sprintf(
			"durable-value-%d", id,
		)

		commands[id] = kvstore.Command{
			Op:       kvstore.OpPut,
			Key:      keys[id],
			Value:    values[id],
			ClientID: "recovery-client",
			RequestID: fmt.Sprintf(
				"durable-put-%d", id,
			),
		}

		cluster.write(
			t,
			id,
			leaders[id],
			commands[id],
		)
	}

	for id, manager := range cluster.nodes {
		if err := manager.Close(); err != nil {
			t.Fatalf(
				"close node %d: %v",
				id,
				err,
			)
		}
	}

	cluster.queue = nil

	t.Log("all five managers closed")

	for id := raft.NodeID(1); id <= 5; id++ {
		manager, err := OpenManager(
			cluster.configs[id],
		)
		if err != nil {
			t.Fatalf(
				"reopen node %d: %v",
				id,
				err,
			)
		}

		cluster.nodes[id] = manager
	}

	// Verify persisted values before new elections.
	for _, placement := range cluster.layout.Shards {
		id := placement.ID

		cluster.requireValue(
			t, id, keys[id], values[id],
		)

		for _, nodeID := range placement.Replicas {
			group := cluster.group(
				t, nodeID, id,
			)

			_, found, err :=
				group.Shard().CheckedRequestResult(
					commands[id],
				)

			if err != nil || !found {
				t.Fatalf(
					"missing recovered request result node=%d shard=%d found=%v err=%v",
					nodeID,
					id,
					found,
					err,
				)
			}
		}
	}

	cluster.requireIsolation(t, keys)

	t.Log("all 12 replicas recovered committed values")
	t.Log("all 12 replicas recovered request identities")

	leaders = cluster.elect(t)

	for _, placement := range cluster.layout.Shards {
		id := placement.ID

		group := cluster.group(
			t, leaders[id], id,
		)

		before, err := group.Shard().Status()
		if err != nil {
			t.Fatal(err)
		}

		// Retry the original committed request.
		if err := group.Propose(
			commands[id],
		); err != nil {
			t.Fatal(err)
		}

		cluster.deliver(t)

		after, err := group.Shard().Status()
		if err != nil {
			t.Fatal(err)
		}

		if after.Commit != before.Commit {
			t.Fatalf(
				"duplicate request advanced shard %d commit index",
				id,
			)
		}

		updated := commands[id]
		updated.RequestID += "-updated"
		updated.Value += "-updated"

		cluster.write(
			t,
			id,
			leaders[id],
			updated,
		)

		t.Logf(
			"shard=%d duplicate retry preserved commit index; new write succeeded",
			id,
		)
	}

	cluster.requireIsolation(t, keys)
}
