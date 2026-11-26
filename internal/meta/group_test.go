package meta

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type metadataTestCluster struct {
	nodes   map[raft.NodeID]*Group
	configs map[raft.NodeID]GroupConfig
	queue   []raft.Message
}

func newMetadataTestCluster(t *testing.T) *metadataTestCluster {
	t.Helper()

	c := &metadataTestCluster{
		nodes:   make(map[raft.NodeID]*Group),
		configs: make(map[raft.NodeID]GroupConfig),
	}

	root := t.TempDir()

	for _, id := range MetadataReplicas() {
		c.configs[id] = GroupConfig{
			ID: id,
			DataDir: filepath.Join(
				root,
				string(rune('0'+id)),
			),
			ElectionTimeout: 10 + int(id)*10,
			Send: func(messages []raft.Message) error {
				for _, message := range messages {
					copyMessage := message

					copyMessage.Entries = append(
						[]raft.Entry(nil),
						message.Entries...,
					)

					for i := range copyMessage.Entries {
						copyMessage.Entries[i].Data = append(
							[]byte(nil),
							message.Entries[i].Data...,
						)
					}

					c.queue = append(
						c.queue,
						copyMessage,
					)
				}

				return nil
			},
		}
	}

	t.Cleanup(func() {
		for _, group := range c.nodes {
			if err := group.Close(); err != nil {
				t.Error(err)
			}
		}
	})

	c.open(t)

	return c
}

func (c *metadataTestCluster) open(t *testing.T) {
	t.Helper()

	for _, id := range MetadataReplicas() {
		group, err := OpenGroup(c.configs[id])
		if err != nil {
			t.Fatalf("open node %d: %v", id, err)
		}

		c.nodes[id] = group
	}
}

func (c *metadataTestCluster) deliver(t *testing.T) {
	t.Helper()

	for round := 0; len(c.queue) > 0; round++ {
		if round >= 1000 {
			t.Fatal("metadata message delivery did not settle")
		}

		batch := c.queue
		c.queue = nil

		for _, message := range batch {
			target := c.nodes[message.To]

			if target == nil {
				t.Fatalf(
					"unexpected destination %d",
					message.To,
				)
			}

			if err := target.Step(message); err != nil {
				t.Fatalf(
					"deliver %d -> %d: %v",
					message.From,
					message.To,
					err,
				)
			}
		}
	}
}

func (c *metadataTestCluster) tick(t *testing.T) {
	t.Helper()

	for _, id := range MetadataReplicas() {
		if err := c.nodes[id].Tick(); err != nil {
			t.Fatalf("tick node %d: %v", id, err)
		}
	}

	c.deliver(t)
}

func (c *metadataTestCluster) elect(t *testing.T) raft.NodeID {
	t.Helper()

	for attempt := 0; attempt < 300; attempt++ {
		c.tick(t)

		var leader raft.NodeID

		for _, id := range MetadataReplicas() {
			state, err := c.nodes[id].Status()
			if err != nil {
				t.Fatal(err)
			}

			if state.Role == raft.Leader {
				if leader != 0 {
					t.Fatal("multiple metadata leaders")
				}

				leader = id
			}
		}

		if leader == 0 {
			continue
		}

		ready := true

		for _, id := range MetadataReplicas() {
			state, applied, err :=
				c.nodes[id].StatusSnapshot()

			if err != nil {
				t.Fatal(err)
			}

			if state.LeaderID != leader ||
				state.Commit == 0 ||
				applied != state.Commit {
				ready = false
			}
		}

		if ready {
			t.Logf("metadata leader=%d", leader)
			return leader
		}
	}

	t.Fatal("metadata leader election failed")
	return 0
}

func (c *metadataTestCluster) requireMap(
	t *testing.T,
	expected ShardMap,
) {
	t.Helper()

	for _, id := range MetadataReplicas() {
		actual, err := c.nodes[id].Current()
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf(
				"node=%d map=%+v expected=%+v",
				id,
				actual,
				expected,
			)
		}

		state, applied, err :=
			c.nodes[id].StatusSnapshot()

		if err != nil {
			t.Fatal(err)
		}

		if applied != state.Commit {
			t.Fatalf(
				"node=%d applied=%d commit=%d",
				id,
				applied,
				state.Commit,
			)
		}

		t.Logf(
			"node=%d metadata version=%d applied=%d",
			id,
			actual.Version,
			applied,
		)
	}
}

func (c *metadataTestCluster) bootstrap(
	t *testing.T,
	leader raft.NodeID,
) {
	t.Helper()

	if err := c.nodes[leader].Bootstrap(); err != nil {
		t.Fatal(err)
	}

	c.deliver(t)

	for i := 0; i < 10; i++ {
		c.tick(t)
	}

	c.requireMap(t, DefaultShardMap())
}

func TestMetadataGroupReplication(t *testing.T) {
	c := newMetadataTestCluster(t)

	for _, group := range c.nodes {
		version, err := group.Version()

		if err != nil || version != 0 {
			t.Fatalf(
				"initial version=%d err=%v",
				version,
				err,
			)
		}
	}

	leader := c.elect(t)

	c.bootstrap(t, leader)

	next := DefaultShardMap()
	next.Version = 2

	command := Command{
		Type:            SetShardMap,
		ExpectedVersion: 1,
		ShardMap:        next,
	}

	for id, group := range c.nodes {
		if id == leader {
			continue
		}

		if err := group.Propose(command); !errors.Is(
			err,
			raft.ErrNotLeader,
		) {
			t.Fatalf(
				"follower %d accepted proposal: %v",
				id,
				err,
			)
		}
	}

	if err := c.nodes[leader].Propose(command); err != nil {
		t.Fatal(err)
	}

	c.deliver(t)

	for i := 0; i < 10; i++ {
		c.tick(t)
	}

	c.requireMap(t, next)

	if err := c.nodes[leader].Propose(command); !errors.Is(
		err,
		ErrStaleVersion,
	) {
		t.Fatalf("stale proposal error=%v", err)
	}

	before, err := c.nodes[leader].Status()
	if err != nil {
		t.Fatal(err)
	}

	if err := c.nodes[leader].Bootstrap(); err != nil {
		t.Fatal(err)
	}

	c.deliver(t)

	after, err := c.nodes[leader].Status()
	if err != nil {
		t.Fatal(err)
	}

	if after.Commit != before.Commit {
		t.Fatal("duplicate bootstrap changed commit index")
	}

	c.requireMap(t, next)

	t.Log("replication and version validation passed")
}

func TestMetadataGroupRecovery(t *testing.T) {
	c := newMetadataTestCluster(t)

	leader := c.elect(t)
	c.bootstrap(t, leader)

	next := DefaultShardMap()
	next.Version = 2

	if err := c.nodes[leader].Propose(Command{
		Type:            SetShardMap,
		ExpectedVersion: 1,
		ShardMap:        next,
	}); err != nil {
		t.Fatal(err)
	}

	c.deliver(t)

	for i := 0; i < 10; i++ {
		c.tick(t)
	}

	c.requireMap(t, next)

	for id, group := range c.nodes {
		if err := group.Close(); err != nil {
			t.Fatalf("close node %d: %v", id, err)
		}
	}

	c.queue = nil

	t.Log("all three metadata replicas closed")

	c.open(t)

	c.requireMap(t, next)

	t.Log("all three replicas recovered committed version 2")

	leader = c.elect(t)

	next.Version = 3

	if err := c.nodes[leader].Propose(Command{
		Type:            SetShardMap,
		ExpectedVersion: 2,
		ShardMap:        next,
	}); err != nil {
		t.Fatal(err)
	}

	c.deliver(t)

	for i := 0; i < 10; i++ {
		c.tick(t)
	}

	c.requireMap(t, next)

	t.Log("metadata replication resumed after recovery")
}

func TestMetadataGroupInvalidConfiguration(t *testing.T) {
	for _, id := range []raft.NodeID{0, 4, 5, 99} {
		group, err := OpenGroup(GroupConfig{
			ID:              id,
			DataDir:         t.TempDir(),
			ElectionTimeout: 10,
			Send: func([]raft.Message) error {
				return nil
			},
		})

		if group != nil {
			_ = group.Close()
		}

		if !errors.Is(err, ErrInvalidMember) {
			t.Fatalf("node=%d error=%v", id, err)
		}
	}
}

func TestMetadataGroupClosed(t *testing.T) {
	c := newMetadataTestCluster(t)
	group := c.nodes[1]

	if err := group.Close(); err != nil {
		t.Fatal(err)
	}

	if err := group.Close(); err != nil {
		t.Fatal(err)
	}

	if err := group.Tick(); !errors.Is(
		err,
		ErrGroupClosed,
	) {
		t.Fatalf("tick error=%v", err)
	}

	if _, err := group.Current(); !errors.Is(
		err,
		ErrGroupClosed,
	) {
		t.Fatalf("current error=%v", err)
	}

	if _, err := group.Status(); !errors.Is(
		err,
		ErrGroupClosed,
	) {
		t.Fatalf("status error=%v", err)
	}
}
