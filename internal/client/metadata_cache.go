package client

import (
	"errors"
	"fmt"
	"sync"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

var (
	ErrMetadataUnavailable = errors.New("metadata unavailable")
	ErrStaleMetadata       = errors.New("stale metadata")
	ErrInvalidMetadataTTL  = errors.New("invalid metadata TTL")
)

// MetadataCache stores immutable routing table snapshots.
//
// Readers can safely continue using an existing snapshot
// while another goroutine publishes updated metadata.
type MetadataCache struct {
	mu        sync.RWMutex
	table     *RoutingTable
	updatedAt time.Time
	ttl       time.Duration
}

// NewMetadataCache creates an initially empty metadata cache.
func NewMetadataCache(
	ttl time.Duration,
) (*MetadataCache, error) {
	if ttl <= 0 {
		return nil, ErrInvalidMetadataTTL
	}

	return &MetadataCache{
		ttl: ttl,
	}, nil
}

// Update validates metadata before publishing it.
//
// Older routing epochs are rejected. Equal epochs are
// accepted because leader information can change without
// changing shard ownership.
func (cache *MetadataCache) Update(
	metadata *shardraftpb.ShardMap,
) error {
	table, err := NewRoutingTable(metadata)

	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.table != nil &&
		table.Epoch() < cache.table.Epoch() {
		return fmt.Errorf(
			"%w: received epoch %d, current epoch %d",
			ErrStaleMetadata,
			table.Epoch(),
			cache.table.Epoch(),
		)
	}

	cache.table = table
	cache.updatedAt = time.Now()

	return nil
}

// Snapshot returns the currently cached routing table.
//
// The returned table is immutable through its public API.
// An expired snapshot remains available for inspection;
// callers should use NeedsRefresh before routing requests.
func (cache *MetadataCache) Snapshot() (
	*RoutingTable,
	error,
) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	if cache.table == nil {
		return nil, ErrMetadataUnavailable
	}

	return cache.table, nil
}

// Epoch returns the currently cached routing epoch.
// Zero indicates that metadata has not been loaded.
func (cache *MetadataCache) Epoch() uint64 {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	if cache.table == nil {
		return 0
	}

	return cache.table.Epoch()
}

// UpdatedAt returns the time of the last successful update.
func (cache *MetadataCache) UpdatedAt() time.Time {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return cache.updatedAt
}

// NeedsRefresh reports whether metadata is missing,
// explicitly invalidated, or expired.
func (cache *MetadataCache) NeedsRefresh() bool {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return cache.table == nil ||
		cache.updatedAt.IsZero() ||
		time.Since(cache.updatedAt) >= cache.ttl
}

// Invalidate marks metadata as requiring refresh.
//
// The existing snapshot and epoch are retained to prevent
// accepting older metadata after invalidation.
func (cache *MetadataCache) Invalidate() {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.updatedAt = time.Time{}
}

// Route resolves a key using the current cached snapshot.
//
// Refresh decisions are handled by the client orchestration
// layer rather than performing network operations here.
func (cache *MetadataCache) Route(
	key string,
) (*shardraftpb.ShardInfo, error) {
	table, err := cache.Snapshot()

	if err != nil {
		return nil, err
	}

	return table.Route(key)
}
