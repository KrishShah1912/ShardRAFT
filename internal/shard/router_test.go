package shard

import (
	"errors"
	"fmt"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/meta"
)

// keyForShard returns a key that hashes into the given shard under the
// supplied shard map.
func keyForShard(t testing.TB, m meta.ShardMap, id meta.ShardID) string {
	t.Helper()

	for i := 0; i < 1000000; i++ {
		key := fmt.Sprintf("probe-%d", i)

		owner, err := m.OwnerForKey(key)
		if err != nil {
			t.Fatal(err)
		}

		if owner.ID == id {
			return key
		}
	}

	t.Fatalf("no key found for shard %d", id)
	return ""
}

func TestRouterRejectsInvalidMap(t *testing.T) {
	invalid := meta.DefaultShardMap()
	invalid.Version = 0

	if _, err := NewRouter(invalid); !errors.Is(
		err,
		meta.ErrInvalidShardMap,
	) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRouterRoutesToOwningShard(t *testing.T) {
	m := meta.DefaultShardMap()

	router, err := NewRouter(m)
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []meta.ShardID{1, 2, 3, 4} {
		key := keyForShard(t, m, id)

		route, err := router.Route(key)
		if err != nil {
			t.Fatal(err)
		}

		if route.GroupID != id {
			t.Fatalf(
				"key %q routed to shard %d, want %d",
				key,
				route.GroupID,
				id,
			)
		}

		if route.Slot != meta.SlotForKey(key) {
			t.Fatalf("route slot mismatch for key %q", key)
		}

		if len(route.Replicas) != 3 {
			t.Fatalf(
				"shard %d has %d replicas, want 3",
				id,
				len(route.Replicas),
			)
		}
	}
}

func TestRouterCheckOwnership(t *testing.T) {
	m := meta.DefaultShardMap()

	router, err := NewRouter(m)
	if err != nil {
		t.Fatal(err)
	}

	key := keyForShard(t, m, 1)

	if err := router.CheckOwnership(1, key); err != nil {
		t.Fatalf("owner rejected its own key: %v", err)
	}

	err = router.CheckOwnership(2, key)

	var wrong *WrongShardError
	if !errors.As(err, &wrong) {
		t.Fatalf("expected *WrongShardError, got %v", err)
	}

	if wrong.Owner != 1 {
		t.Fatalf("wrong owner reported: %d", wrong.Owner)
	}

	if len(wrong.Replicas) != 3 {
		t.Fatalf("wrong error omitted replicas: %+v", wrong)
	}
}

func TestRouterOwnershipFollowsMapUpdate(t *testing.T) {
	m := meta.DefaultShardMap()

	router, err := NewRouter(m)
	if err != nil {
		t.Fatal(err)
	}

	key := keyForShard(t, m, 1)

	if err := router.CheckOwnership(1, key); err != nil {
		t.Fatalf("shard 1 did not own the key initially: %v", err)
	}

	// Reassign the entire slot space to shard 1 at the next version.
	updated := meta.ShardMap{
		Version: m.Version + 1,
		Shards: []meta.Shard{
			{
				ID: 2,
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

	// Ownership now follows the updated map.
	if err := router.CheckOwnership(2, key); err != nil {
		t.Fatalf("ownership did not follow the map update: %v", err)
	}

	err = router.CheckOwnership(1, key)

	var wrong *WrongShardError
	if !errors.As(err, &wrong) || wrong.Owner != 2 {
		t.Fatalf("stale ownership after update: %v", err)
	}
}
