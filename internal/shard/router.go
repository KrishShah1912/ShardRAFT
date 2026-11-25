package shard

import (
	"fmt"
	"sync"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

// Route describes the placement of a key within the shard map.
type Route struct {
	Slot     uint32
	GroupID  meta.ShardID
	Replicas []raft.NodeID
}

// WrongShardError reports that a key is not owned by the shard that
// received it. It names the real owning group and its replicas.
type WrongShardError struct {
	Key      string
	Slot     uint32
	Owner    meta.ShardID
	Replicas []raft.NodeID
}

func (e *WrongShardError) Error() string {
	return fmt.Sprintf(
		"key %q (slot %d) is owned by shard %d, not this shard",
		e.Key,
		e.Slot,
		e.Owner,
	)
}

// Router resolves keys to their owning shard using the current shard
// map. It is safe for concurrent use and the map may be replaced at
// any time.
type Router struct {
	mu       sync.RWMutex
	shardMap meta.ShardMap
}

// NewRouter returns a router backed by a validated shard map.
func NewRouter(shardMap meta.ShardMap) (*Router, error) {
	if err := shardMap.Validate(); err != nil {
		return nil, err
	}

	return &Router{shardMap: shardMap.Clone()}, nil
}

// SetShardMap replaces the router's shard map after validation.
func (r *Router) SetShardMap(shardMap meta.ShardMap) error {
	if err := shardMap.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.shardMap = shardMap.Clone()
	return nil
}

// ShardMap returns an independent copy of the current shard map.
func (r *Router) ShardMap() meta.ShardMap {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.shardMap.Clone()
}

// Route maps a key to its slot, owning group, and replicas.
func (r *Router) Route(key string) (Route, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	slot := meta.SlotForKey(key)

	owner, err := r.shardMap.OwnerForSlot(slot)
	if err != nil {
		return Route{}, err
	}

	return Route{
		Slot:     slot,
		GroupID:  owner.ID,
		Replicas: owner.Replicas,
	}, nil
}

// CheckOwnership reports whether a group owns a key under the current
// shard map. It returns a *WrongShardError naming the real owner when
// the group does not own the key.
func (r *Router) CheckOwnership(groupID meta.ShardID, key string) error {
	route, err := r.Route(key)
	if err != nil {
		return err
	}

	if route.GroupID != groupID {
		return &WrongShardError{
			Key:      key,
			Slot:     route.Slot,
			Owner:    route.GroupID,
			Replicas: route.Replicas,
		}
	}

	return nil
}
