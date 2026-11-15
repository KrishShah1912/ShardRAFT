package kvstore

// Engine provides local key-value storage.
//
// The Raft log and snapshots remain authoritative.
// This engine is a rebuildable materialized view.
type Engine interface {
	Put(key, value string) error
	Get(key string) (string, bool, error)
	Delete(key string) error
	Clear() error
	Close() error
}
