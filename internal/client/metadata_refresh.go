package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

var (
	ErrInvalidRefreshConfig = errors.New("invalid metadata refresh configuration")
	ErrMetadataRefresh      = errors.New("metadata refresh failed")
	ErrNotWrongShard        = errors.New("expected WRONG_SHARD response")
)

// MetadataFetcher retrieves metadata from a specified endpoint.
type MetadataFetcher func(
	context.Context,
	string,
) (*shardraftpb.ShardMap, error)

// MetadataRefresher coordinates metadata discovery and refresh.
type MetadataRefresher struct {
	cache          *MetadataCache
	endpoints      []string
	attemptTimeout time.Duration
	fetch          MetadataFetcher
	gate           chan struct{}
}

func NewMetadataRefresher(
	cache *MetadataCache,
	endpoints []string,
	attemptTimeout time.Duration,
	fetch MetadataFetcher,
) (*MetadataRefresher, error) {
	if cache == nil ||
		fetch == nil ||
		attemptTimeout <= 0 ||
		len(endpoints) == 0 {
		return nil, ErrInvalidRefreshConfig
	}

	addresses := make([]string, 0, len(endpoints))
	seen := make(map[string]bool)

	for _, endpoint := range endpoints {
		address, err := normalizeEndpoint(endpoint)

		if err != nil {
			return nil, fmt.Errorf(
				"%w: %v",
				ErrInvalidRefreshConfig,
				err,
			)
		}

		if !seen[address] {
			addresses = append(addresses, address)
			seen[address] = true
		}
	}

	return &MetadataRefresher{
		cache:          cache,
		endpoints:      addresses,
		attemptTimeout: attemptTimeout,
		fetch:          fetch,
		gate:           make(chan struct{}, 1),
	}, nil
}

// NewGRPCMetadataRefresher connects discovery to the existing
// gRPC connection manager.
func NewGRPCMetadataRefresher(
	cache *MetadataCache,
	connections *ConnectionManager,
	endpoints []string,
	attemptTimeout time.Duration,
) (*MetadataRefresher, error) {
	if connections == nil {
		return nil, ErrInvalidRefreshConfig
	}

	return NewMetadataRefresher(
		cache,
		endpoints,
		attemptTimeout,
		func(
			ctx context.Context,
			address string,
		) (*shardraftpb.ShardMap, error) {
			client, err := connections.MetaClient(address)

			if err != nil {
				return nil, err
			}

			response, err := client.GetShardMap(
				ctx,
				&shardraftpb.GetShardMapRequest{},
			)

			if err != nil {
				return nil, err
			}

			return response.GetShardMap(), nil
		},
	)
}

// Ensure refreshes missing, expired, or invalidated metadata.
func (r *MetadataRefresher) Ensure(
	ctx context.Context,
) error {
	return r.refresh(ctx, false, nil)
}

// Refresh explicitly requests a fresh metadata snapshot.
func (r *MetadataRefresher) Refresh(
	ctx context.Context,
) error {
	return r.refresh(ctx, true, nil)
}

// HandleWrongShard refreshes metadata after a routing rejection.
//
// observed is the routing table used by the rejected request.
// If another request has already refreshed that snapshot,
// the additional network refresh can be avoided.
func (r *MetadataRefresher) HandleWrongShard(
	ctx context.Context,
	observed *RoutingTable,
	failure *shardraftpb.RoutingError,
) error {
	if failure == nil ||
		failure.GetCode() !=
			shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD {
		return ErrNotWrongShard
	}

	return r.refresh(ctx, true, observed)
}

func (r *MetadataRefresher) refresh(
	ctx context.Context,
	force bool,
	observed *RoutingTable,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if !force && !r.cache.NeedsRefresh() {
		return nil
	}

	// Serialize refresh operations while allowing waiting
	// callers to cancel independently.
	select {
	case r.gate <- struct{}{}:
		defer func() {
			<-r.gate
		}()
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if !force && !r.cache.NeedsRefresh() {
		return nil
	}

	if observed != nil {
		current, err := r.cache.Snapshot()

		if err == nil &&
			current != observed &&
			!r.cache.NeedsRefresh() {
			return nil
		}
	}

	if force {
		r.cache.Invalidate()
	}

	return r.fetchAndPublish(ctx)
}

// candidateEndpoints combines configured bootstrap addresses
// with previously discovered replica addresses.
func (r *MetadataRefresher) candidateEndpoints() []string {
	addresses := make([]string, 0, len(r.endpoints))
	seen := make(map[string]bool)

	add := func(endpoint string) {
		address, err := normalizeEndpoint(endpoint)

		if err != nil || seen[address] {
			return
		}

		addresses = append(addresses, address)
		seen[address] = true
	}

	for _, endpoint := range r.endpoints {
		add(endpoint)
	}

	table, err := r.cache.Snapshot()

	if err != nil {
		return addresses
	}

	// Production currently has one shard covering all slots.
	// Iterate discovered metadata without introducing
	// additional server-side Raft groups.
	for _, shard := range table.shards {
		for _, replica := range shard.GetReplicas() {
			add(replica.GetAddress())
		}
	}

	return addresses
}

func (r *MetadataRefresher) fetchAndPublish(
	ctx context.Context,
) error {
	failures := []error{ErrMetadataRefresh}

	for _, address := range r.candidateEndpoints() {
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptCtx, cancel := context.WithTimeout(
			ctx,
			r.attemptTimeout,
		)

		metadata, err := r.fetch(attemptCtx, address)

		attemptErr := attemptCtx.Err()
		cancel()

		if err := ctx.Err(); err != nil {
			return err
		}

		if err == nil && attemptErr != nil {
			err = attemptErr
		}

		if err == nil {
			err = r.cache.Update(metadata)
		}

		if err == nil {
			return nil
		}

		failures = append(
			failures,
			fmt.Errorf("%s: %w", address, err),
		)
	}

	return errors.Join(failures...)
}

// Route ensures metadata is available before resolving a key.
func (r *MetadataRefresher) Route(
	ctx context.Context,
	key string,
) (*shardraftpb.ShardInfo, error) {
	if err := r.Ensure(ctx); err != nil {
		return nil, err
	}

	return r.cache.Route(key)
}
