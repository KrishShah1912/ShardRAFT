package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/meta"
)

func putCommand(key, value string) kvstore.Command {
	return kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       key,
		Value:     value,
		ClientID:  "client-1",
		RequestID: "request-" + key,
	}
}

func TestGroupAcceptsOwnedKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)
	defer s.Close()
	electTestLeader(t, s)

	m := meta.DefaultShardMap()
	router, err := NewRouter(m)
	if err != nil {
		t.Fatal(err)
	}

	group := NewGroup(1, s, router)
	key := keyForShard(t, m, 1)

	if err := group.Propose(putCommand(key, "owned")); err != nil {
		t.Fatalf("group rejected a key it owns: %v", err)
	}

	value, exists, err := group.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !exists || value != "owned" {
		t.Fatalf("unexpected value: %q, exists=%v", value, exists)
	}
}

func TestGroupRejectsForeignKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)
	defer s.Close()
	electTestLeader(t, s)

	m := meta.DefaultShardMap()
	router, err := NewRouter(m)
	if err != nil {
		t.Fatal(err)
	}

	group := NewGroup(1, s, router)
	foreign := keyForShard(t, m, 2)

	err = group.Propose(putCommand(foreign, "nope"))

	var wrong *WrongShardError
	if !errors.As(err, &wrong) {
		t.Fatalf("expected *WrongShardError, got %v", err)
	}

	if wrong.Owner != 2 {
		t.Fatalf("wrong owner reported: %d", wrong.Owner)
	}

	// The foreign write must not have reached the store.
	if _, exists, _ := s.Get(foreign); exists {
		t.Fatal("rejected write reached the store")
	}

	if _, _, err := group.Get(foreign); !errors.As(err, &wrong) {
		t.Fatalf("Get did not reject a foreign key: %v", err)
	}
}

func TestGroupOwnershipFollowsMapUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)
	defer s.Close()
	electTestLeader(t, s)

	m := meta.DefaultShardMap()
	router, err := NewRouter(m)
	if err != nil {
		t.Fatal(err)
	}

	group := NewGroup(1, s, router)
	key := keyForShard(t, m, 2)

	// Initially owned by shard 2, so group 1 rejects it.
	if err := group.Propose(putCommand(key, "v1")); err == nil {
		t.Fatal("group accepted a key it does not own")
	}

	// Reassign all slots to group 1 at the next version.
	updated := meta.ShardMap{
		Version: m.Version + 1,
		Shards: []meta.Shard{
			{
				ID: 1,
				Range: meta.SlotRange{
					Start: 0,
					End:   meta.TotalSlots - 1,
				},
				Replicas: m.Shards[0].Replicas,
			},
		},
	}

	if err := router.SetShardMap(updated); err != nil {
		t.Fatal(err)
	}

	// The same key is now owned by group 1 on every call.
	if err := group.Propose(putCommand(key, "v2")); err != nil {
		t.Fatalf("group rejected a key it now owns: %v", err)
	}

	value, exists, err := group.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !exists || value != "v2" {
		t.Fatalf("unexpected value: %q, exists=%v", value, exists)
	}
}
