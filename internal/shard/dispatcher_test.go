package shard

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/meta"
)

// localGroupForShard builds an elected single-node group hosting the
// given shard ID, backed by the supplied router.
func localGroupForShard(
	t *testing.T,
	id meta.ShardID,
	router *Router,
) *Group {
	t.Helper()

	path := filepath.Join(t.TempDir(), "node.wal")
	s := openTestShard(t, path)
	t.Cleanup(func() { s.Close() })
	electTestLeader(t, s)

	return NewGroup(id, s, router)
}

func TestDispatcherRoutesToLocalGroup(t *testing.T) {
	m := meta.DefaultShardMap()

	router, err := NewRouter(m)
	if err != nil {
		t.Fatal(err)
	}

	dispatcher := NewDispatcher(router)

	group1 := localGroupForShard(t, 1, router)
	group2 := localGroupForShard(t, 2, router)
	dispatcher.AddGroup(group1)
	dispatcher.AddGroup(group2)

	key1 := keyForShard(t, m, 1)
	key2 := keyForShard(t, m, 2)

	if err := dispatcher.Propose(putCommand(key1, "one")); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Propose(putCommand(key2, "two")); err != nil {
		t.Fatal(err)
	}

	// Each write landed in the group that owns the key.
	if value, exists, _ := group1.Shard().Get(key1); !exists ||
		value != "one" {
		t.Fatalf("key1 not stored in group 1: %q, %v", value, exists)
	}
	if value, exists, _ := group2.Shard().Get(key2); !exists ||
		value != "two" {
		t.Fatalf("key2 not stored in group 2: %q, %v", value, exists)
	}

	// The write for key2 must not have reached group 1.
	if _, exists, _ := group1.Shard().Get(key2); exists {
		t.Fatal("key2 leaked into group 1")
	}

	// Reads route the same way.
	if value, _, err := dispatcher.Get(key1); err != nil ||
		value != "one" {
		t.Fatalf("dispatcher read key1 = %q, err=%v", value, err)
	}
}

func TestDispatcherRejectsNonLocalKey(t *testing.T) {
	m := meta.DefaultShardMap()

	router, err := NewRouter(m)
	if err != nil {
		t.Fatal(err)
	}

	dispatcher := NewDispatcher(router)
	dispatcher.AddGroup(localGroupForShard(t, 1, router))

	// Shard 3 is not hosted locally.
	foreign := keyForShard(t, m, 3)

	err = dispatcher.Propose(putCommand(foreign, "nope"))

	var notLocal *NotLocalError
	if !errors.As(err, &notLocal) {
		t.Fatalf("expected *NotLocalError, got %v", err)
	}

	if notLocal.Owner != 3 {
		t.Fatalf("wrong owner reported: %d", notLocal.Owner)
	}

	if len(notLocal.Replicas) != 3 {
		t.Fatalf("not-local error omitted replicas: %+v", notLocal)
	}

	if _, _, err := dispatcher.Get(foreign); !errors.As(
		err,
		&notLocal,
	) {
		t.Fatalf("Get did not reject a non-local key: %v", err)
	}
}
