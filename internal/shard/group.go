package shard

import (
	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/meta"
)

// Group wraps a Shard with ownership enforcement. Every operation is
// checked against the router's current shard map, so a group rejects
// keys it does not own even after the map changes.
type Group struct {
	id     meta.ShardID
	shard  *Shard
	router *Router
}

// NewGroup wraps a shard as the replication group with the given ID.
func NewGroup(id meta.ShardID, shard *Shard, router *Router) *Group {
	return &Group{
		id:     id,
		shard:  shard,
		router: router,
	}
}

// ID returns the group's shard identifier.
func (g *Group) ID() meta.ShardID {
	return g.id
}

// Shard returns the wrapped shard.
func (g *Group) Shard() *Shard {
	return g.shard
}

// Propose applies a command only when this group owns the key. It
// returns a *WrongShardError naming the real owner otherwise.
func (g *Group) Propose(command kvstore.Command) error {
	if err := g.router.CheckOwnership(g.id, command.Key); err != nil {
		return err
	}

	return g.shard.Propose(command)
}

// Get reads a key only when this group owns it. It returns a
// *WrongShardError naming the real owner otherwise.
func (g *Group) Get(key string) (string, bool, error) {
	if err := g.router.CheckOwnership(g.id, key); err != nil {
		return "", false, err
	}

	return g.shard.Get(key)
}
