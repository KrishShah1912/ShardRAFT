package shard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func managerTestConfig(
	t *testing.T,
	nodeID raft.NodeID,
) ManagerConfig {
	t.Helper()

	return ManagerConfig{
		NodeID:          nodeID,
		DataDir:         t.TempDir(),
		ElectionTimeout: 10,
		ShardMap:        meta.DefaultShardMap(),
		Send: func(
			_ meta.ShardID,
			_ []raft.Message,
		) error {
			return nil
		},
	}
}

func openTestManager(
	t *testing.T,
	cfg ManagerConfig,
) *Manager {
	t.Helper()

	manager, err := OpenManager(cfg)
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

func TestManagerReplicaPlacement(t *testing.T) {
	cases := []struct {
		nodeID raft.NodeID
		groups []meta.ShardID
	}{
		{1, []meta.ShardID{1, 4}},
		{2, []meta.ShardID{1, 2}},
		{3, []meta.ShardID{1, 2, 3}},
		{4, []meta.ShardID{2, 3, 4}},
		{5, []meta.ShardID{3, 4}},
	}

	total := 0

	for _, tc := range cases {
		t.Run(fmt.Sprintf("node-%d", tc.nodeID),
			func(t *testing.T) {
				cfg := managerTestConfig(t, tc.nodeID)
				manager := openTestManager(t, cfg)

				ids := manager.GroupIDs()

				if !reflect.DeepEqual(ids, tc.groups) {
					t.Fatalf(
						"groups=%v want=%v",
						ids,
						tc.groups,
					)
				}

				total += len(ids)

				for _, id := range ids {
					group, err := manager.Group(id)
					if err != nil {
						t.Fatal(err)
					}

					status, err := group.Shard().Status()
					if err != nil {
						t.Fatal(err)
					}

					if status.ID != tc.nodeID {
						t.Fatalf(
							"node ID=%d want=%d",
							status.ID,
							tc.nodeID,
						)
					}

					dir := filepath.Join(
						cfg.DataDir,
						fmt.Sprintf("shard-%d", id),
					)

					info, err := os.Stat(dir)
					if err != nil {
						t.Fatal(err)
					}

					if !info.IsDir() {
						t.Fatalf(
							"not a directory: %s",
							dir,
						)
					}
				}

				t.Logf(
					"node=%d local groups=%v",
					tc.nodeID,
					ids,
				)
			},
		)
	}

	if total != 12 {
		t.Fatalf("replicas=%d want=12", total)
	}
}

func TestManagerKeyRouting(t *testing.T) {
	manager := openTestManager(
		t,
		managerTestConfig(t, 1),
	)

	seen := make(map[meta.ShardID]bool)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("manager-key-%d", i)

		slot := meta.SlotForKey(key)
		expected := meta.ShardID(slot/16384 + 1)

		if meta.SlotForKey(key) != slot {
			t.Fatalf("unstable slot for %q", key)
		}

		seen[expected] = true

		group, err := manager.GroupForKey(key)

		if expected == 1 || expected == 4 {
			if err != nil {
				t.Fatal(err)
			}

			if group.ID() != expected {
				t.Fatalf(
					"key=%q group=%d want=%d",
					key,
					group.ID(),
					expected,
				)
			}
		} else {
			var notLocal *NotLocalError

			if !errors.As(err, &notLocal) {
				t.Fatalf(
					"expected non-local error: %v",
					err,
				)
			}

			if notLocal.Owner != expected ||
				notLocal.Slot != slot {
				t.Fatalf(
					"incorrect routing error: %+v",
					notLocal,
				)
			}
		}
	}

	if len(seen) != 4 {
		t.Fatalf("covered only %d shards", len(seen))
	}

	t.Log("1000 keys routed across all four shards")
}

func TestManagerGroupIsolation(t *testing.T) {
	manager := openTestManager(
		t,
		managerTestConfig(t, 1),
	)

	first, err := manager.Group(1)
	if err != nil {
		t.Fatal(err)
	}

	fourth, err := manager.Group(4)
	if err != nil {
		t.Fatal(err)
	}

	if first.Shard() == fourth.Shard() {
		t.Fatal("groups share the same shard instance")
	}

	before, err := fourth.Shard().Status()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		if err := manager.Tick(1); err != nil {
			t.Fatal(err)
		}
	}

	after, err := fourth.Shard().Status()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(before, after) {
		t.Fatal("ticking shard 1 changed shard 4")
	}

	status, err := first.Shard().Status()
	if err != nil {
		t.Fatal(err)
	}

	if status.Term == 0 {
		t.Fatal("shard 1 did not start an election")
	}

	if _, err := manager.Group(2); !errors.Is(
		err, ErrGroupNotLocal,
	) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManagerCloseAndReopen(t *testing.T) {
	cfg := managerTestConfig(t, 3)

	manager := openTestManager(t, cfg)

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := manager.Group(1); !errors.Is(
		err, ErrClosed,
	) {
		t.Fatalf("unexpected error: %v", err)
	}

	reopened := openTestManager(t, cfg)

	expected := []meta.ShardID{1, 2, 3}

	if !reflect.DeepEqual(
		reopened.GroupIDs(),
		expected,
	) {
		t.Fatalf(
			"unexpected recovered groups: %v",
			reopened.GroupIDs(),
		)
	}
}

func TestManagerRejectsInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name   string
		change func(*ManagerConfig)
	}{
		{"zero-node", func(c *ManagerConfig) {
			c.NodeID = 0
		}},
		{"unknown-node", func(c *ManagerConfig) {
			c.NodeID = 6
		}},
		{"empty-directory", func(c *ManagerConfig) {
			c.DataDir = ""
		}},
		{"invalid-timeout", func(c *ManagerConfig) {
			c.ElectionTimeout = 0
		}},
		{"missing-send", func(c *ManagerConfig) {
			c.Send = nil
		}},
		{"invalid-map", func(c *ManagerConfig) {
			c.ShardMap.Version = 0
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := managerTestConfig(t, 1)
			tc.change(&cfg)

			manager, err := OpenManager(cfg)

			if manager != nil {
				_ = manager.Close()
			}

			if err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
