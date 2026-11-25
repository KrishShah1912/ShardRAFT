package shard

import (
	"fmt"
	"sync"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

// NotLocalError reports that the group owning a key is not hosted
// locally. It carries the replicas that can serve the request.
type NotLocalError struct {
	Key      string
	Slot     uint32
	Owner    meta.ShardID
	Replicas []raft.NodeID
}

func (e *NotLocalError) Error() string {
	return fmt.Sprintf(
		"key %q (slot %d) is owned by non-local shard %d",
		e.Key,
		e.Slot,
		e.Owner,
	)
}

// Dispatcher routes requests to the local group that owns each key,
// using the current shard map. Keys owned by groups that are not
// hosted locally are rejected with a *NotLocalError.
type Dispatcher struct {
	router *Router

	mu     sync.RWMutex
	groups map[meta.ShardID]*Group
}

// NewDispatcher returns a dispatcher backed by the given router.
func NewDispatcher(router *Router) *Dispatcher {
	return &Dispatcher{
		router: router,
		groups: make(map[meta.ShardID]*Group),
	}
}

// AddGroup registers a locally hosted group.
func (d *Dispatcher) AddGroup(group *Group) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.groups[group.ID()] = group
}

// localGroup resolves the local group owning a key, or a *NotLocalError
// naming the owning replicas when the group is not hosted locally.
func (d *Dispatcher) localGroup(key string) (*Group, error) {
	route, err := d.router.Route(key)
	if err != nil {
		return nil, err
	}

	d.mu.RLock()
	group, local := d.groups[route.GroupID]
	d.mu.RUnlock()

	if !local {
		return nil, &NotLocalError{
			Key:      key,
			Slot:     route.Slot,
			Owner:    route.GroupID,
			Replicas: route.Replicas,
		}
	}

	return group, nil
}

// Propose dispatches a mutation to the local owning group.
func (d *Dispatcher) Propose(command kvstore.Command) error {
	group, err := d.localGroup(command.Key)
	if err != nil {
		return err
	}

	return group.Propose(command)
}

// Get dispatches a read to the local owning group.
func (d *Dispatcher) Get(key string) (string, bool, error) {
	group, err := d.localGroup(key)
	if err != nil {
		return "", false, err
	}

	return group.Get(key)
}
