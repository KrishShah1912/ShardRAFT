package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

const refreshTestEndpoint = "127.0.0.1:9091"

func refreshTestMap(
	epoch uint64,
	leader uint64,
) *shardraftpb.ShardMap {
	metadata := testShardMap()
	metadata.Epoch = epoch
	metadata.Shards[0].Epoch = epoch
	metadata.Shards[0].LeaderId = leader
	return metadata
}

func refreshTestWrongShard() *shardraftpb.RoutingError {
	return &shardraftpb.RoutingError{
		Code:    shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
		ShardId: 1,
		Epoch:   1,
	}
}

func newTestMetadataRefresher(
	t testing.TB,
	cache *MetadataCache,
	endpoints []string,
	fetch MetadataFetcher,
) *MetadataRefresher {
	t.Helper()

	refresher, err := NewMetadataRefresher(
		cache,
		endpoints,
		time.Second,
		fetch,
	)

	if err != nil {
		t.Fatal(err)
	}

	return refresher
}

func TestMetadataRefreshInvalidConfiguration(t *testing.T) {
	cache := newTestMetadataCache(t)

	fetch := func(
		context.Context,
		string,
	) (*shardraftpb.ShardMap, error) {
		return testShardMap(), nil
	}

	tests := []struct {
		name      string
		cache     *MetadataCache
		endpoints []string
		timeout   time.Duration
		fetch     MetadataFetcher
	}{
		{"NilCache", nil, []string{refreshTestEndpoint}, time.Second, fetch},
		{"NoEndpoints", cache, nil, time.Second, fetch},
		{"InvalidEndpoint", cache, []string{"invalid"}, time.Second, fetch},
		{"ZeroTimeout", cache, []string{refreshTestEndpoint}, 0, fetch},
		{"NegativeTimeout", cache, []string{refreshTestEndpoint}, -time.Second, fetch},
		{"NilFetcher", cache, []string{refreshTestEndpoint}, time.Second, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			refresher, err := NewMetadataRefresher(
				tt.cache,
				tt.endpoints,
				tt.timeout,
				tt.fetch,
			)

			if refresher != nil ||
				!errors.Is(err, ErrInvalidRefreshConfig) {
				t.Fatalf("refresher=%v err=%v", refresher, err)
			}
		})
	}
}

func TestMetadataRefreshInitialDiscovery(t *testing.T) {
	cache := newTestMetadataCache(t)
	calls := 0

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(ctx context.Context, address string) (*shardraftpb.ShardMap, error) {
			calls++

			if address != refreshTestEndpoint {
				t.Fatal("unexpected endpoint")
			}

			return testShardMap(), nil
		},
	)

	if err := refresher.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}

	if calls != 1 || cache.Epoch() != 1 || cache.NeedsRefresh() {
		t.Fatal("initial discovery failed")
	}
}

func TestMetadataRefreshFreshCache(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 1, 1)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			t.Fatal("fresh cache triggered network request")
			return nil, nil
		},
	)

	if err := refresher.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataRefreshExpiredCache(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 1, 1)

	cache.mu.Lock()
	cache.updatedAt = time.Now().Add(-2 * time.Minute)
	cache.mu.Unlock()

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return refreshTestMap(2, 2), nil
		},
	)

	if err := refresher.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}

	if cache.Epoch() != 2 || cache.NeedsRefresh() {
		t.Fatal("expired cache was not refreshed")
	}
}

func TestMetadataRefreshInvalidatedCache(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 1, 1)
	cache.Invalidate()

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return refreshTestMap(1, 2), nil
		},
	)

	if err := refresher.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}

	shard, err := cache.Route("alpha")

	if err != nil || shard.GetLeaderId() != 2 || cache.NeedsRefresh() {
		t.Fatal("invalidated cache was not refreshed")
	}
}

func TestMetadataRefreshForced(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 1, 1)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return refreshTestMap(1, 3), nil
		},
	)

	if err := refresher.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	shard, err := cache.Route("alpha")

	if err != nil || shard.GetLeaderId() != 3 {
		t.Fatal("forced refresh failed")
	}
}

func TestMetadataRefreshWrongShard(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 1, 1)

	observed, err := cache.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return refreshTestMap(2, 2), nil
		},
	)

	err = refresher.HandleWrongShard(
		context.Background(),
		observed,
		refreshTestWrongShard(),
	)

	if err != nil {
		t.Fatal(err)
	}

	shard, err := cache.Route("alpha")

	if err != nil ||
		shard.GetShardId() != 1 ||
		shard.GetEpoch() != 2 {
		t.Fatal("incorrect refreshed route")
	}
}

func TestMetadataRefreshRejectsOtherErrors(t *testing.T) {
	cache := newTestMetadataCache(t)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			t.Fatal("unexpected refresh")
			return nil, nil
		},
	)

	for _, failure := range []*shardraftpb.RoutingError{
		nil,
		{Code: shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER},
		{Code: shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST},
		{Code: shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN},
	} {
		err := refresher.HandleWrongShard(
			context.Background(),
			nil,
			failure,
		)

		if !errors.Is(err, ErrNotWrongShard) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestMetadataRefreshBootstrapFallback(t *testing.T) {
	cache := newTestMetadataCache(t)
	calls := 0

	refresher := newTestMetadataRefresher(
		t, cache,
		[]string{
			"127.0.0.1:9091",
			"127.0.0.1:9092",
		},
		func(ctx context.Context, address string) (*shardraftpb.ShardMap, error) {
			calls++

			if address == "127.0.0.1:9091" {
				return nil, errors.New("endpoint unavailable")
			}

			return testShardMap(), nil
		},
	)

	if err := refresher.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}

	if calls != 2 || cache.Epoch() != 1 {
		t.Fatal("bootstrap fallback failed")
	}
}

func TestMetadataRefreshDiscoveredReplicaFallback(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 1, 1)

	calls := 0

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(ctx context.Context, address string) (*shardraftpb.ShardMap, error) {
			calls++

			if address != "127.0.0.1:9092" {
				return nil, errors.New("endpoint unavailable")
			}

			return refreshTestMap(1, 2), nil
		},
	)

	if err := refresher.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	if calls != 2 {
		t.Fatalf("unexpected call count: %d", calls)
	}

	shard, err := cache.Route("alpha")

	if err != nil || shard.GetLeaderId() != 2 {
		t.Fatal("discovered replica fallback failed")
	}
}

func TestMetadataRefreshDuplicateEndpoints(t *testing.T) {
	cache := newTestMetadataCache(t)

	refresher := newTestMetadataRefresher(
		t, cache,
		[]string{
			refreshTestEndpoint,
			refreshTestEndpoint,
			"127.0.0.1:9092",
		},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return testShardMap(), nil
		},
	)

	if len(refresher.candidateEndpoints()) != 2 {
		t.Fatal("duplicate endpoints were retained")
	}
}

func TestMetadataRefreshRejectsStaleMap(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 3, 1)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return refreshTestMap(2, 2), nil
		},
	)

	err := refresher.Refresh(context.Background())

	if !errors.Is(err, ErrMetadataRefresh) ||
		!errors.Is(err, ErrStaleMetadata) {
		t.Fatalf("unexpected error: %v", err)
	}

	if cache.Epoch() != 3 || !cache.NeedsRefresh() {
		t.Fatal("stale metadata changed the cache")
	}
}

func TestMetadataRefreshInvalidMapFallback(t *testing.T) {
	cache := newTestMetadataCache(t)

	refresher := newTestMetadataRefresher(
		t, cache,
		[]string{
			refreshTestEndpoint,
			"127.0.0.1:9092",
		},
		func(ctx context.Context, address string) (*shardraftpb.ShardMap, error) {
			if address == refreshTestEndpoint {
				return &shardraftpb.ShardMap{}, nil
			}

			return testShardMap(), nil
		},
	)

	if err := refresher.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}

	if cache.Epoch() != 1 {
		t.Fatal("valid fallback metadata was not published")
	}
}

func TestMetadataRefreshAllEndpointsFail(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 1, 1)

	before, _ := cache.Snapshot()
	unavailable := errors.New("unavailable")

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return nil, unavailable
		},
	)

	err := refresher.Refresh(context.Background())

	if !errors.Is(err, ErrMetadataRefresh) ||
		!errors.Is(err, unavailable) {
		t.Fatalf("unexpected error: %v", err)
	}

	after, _ := cache.Snapshot()

	if before != after || !cache.NeedsRefresh() {
		t.Fatal("failed refresh replaced the snapshot")
	}
}

func TestMetadataRefreshCanceledContext(t *testing.T) {
	cache := newTestMetadataCache(t)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			t.Fatal("unexpected fetch")
			return nil, nil
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := refresher.Ensure(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMetadataRefreshExpiredDeadline(t *testing.T) {
	cache := newTestMetadataCache(t)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			t.Fatal("unexpected fetch")
			return nil, nil
		},
	)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	if err := refresher.Ensure(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMetadataRefreshAttemptTimeout(t *testing.T) {
	cache := newTestMetadataCache(t)

	refresher, err := NewMetadataRefresher(
		cache,
		[]string{
			refreshTestEndpoint,
			"127.0.0.1:9092",
		},
		20*time.Millisecond,
		func(ctx context.Context, address string) (*shardraftpb.ShardMap, error) {
			if address == refreshTestEndpoint {
				<-ctx.Done()
				return nil, ctx.Err()
			}

			return testShardMap(), nil
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if err := refresher.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}

	if cache.Epoch() != 1 {
		t.Fatal("timeout fallback failed")
	}
}

func TestMetadataRefreshConcurrentDiscovery(t *testing.T) {
	cache := newTestMetadataCache(t)

	var calls atomic.Int64
	var wg sync.WaitGroup

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			calls.Add(1)
			return testShardMap(), nil
		},
	)

	const workers = 64

	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			if err := refresher.Ensure(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("expected one fetch, got %d", calls.Load())
	}

	t.Logf("%d concurrent callers shared one discovery", workers)
}

func TestMetadataRefreshConcurrentWrongShard(t *testing.T) {
	cache := newTestMetadataCache(t)
	requireMetadataUpdate(t, cache, 1, 1)

	observed, _ := cache.Snapshot()

	var calls atomic.Int64
	var wg sync.WaitGroup

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			calls.Add(1)
			return refreshTestMap(2, 2), nil
		},
	)

	const workers = 64

	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			err := refresher.HandleWrongShard(
				context.Background(),
				observed,
				refreshTestWrongShard(),
			)

			if err != nil {
				t.Error(err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if calls.Load() != 1 || cache.Epoch() != 2 {
		t.Fatalf("calls=%d epoch=%d", calls.Load(), cache.Epoch())
	}

	t.Logf("%d WRONG_SHARD responses shared one refresh", workers)
}

func TestMetadataRefreshWaitingCallerCancellation(t *testing.T) {
	cache := newTestMetadataCache(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(ctx context.Context, address string) (*shardraftpb.ShardMap, error) {
			close(entered)

			select {
			case <-release:
				return testShardMap(), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	)

	go func() {
		done <- refresher.Ensure(context.Background())
	}()

	<-entered

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()

	err := refresher.Ensure(ctx)

	close(release)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected waiting error: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if cache.Epoch() != 1 {
		t.Fatal("waiting caller canceled active refresh")
	}
}

func TestMetadataRefreshRoute(t *testing.T) {
	cache := newTestMetadataCache(t)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return testShardMap(), nil
		},
	)

	for _, key := range []string{
		"alpha",
		"beta",
		"user:123",
		"session:456",
		"你好",
	} {
		shard, err := refresher.Route(context.Background(), key)

		if err != nil ||
			shard.GetShardId() != 1 ||
			shard.GetEpoch() != 1 {
			t.Fatalf("key=%q shard=%v err=%v", key, shard, err)
		}
	}
}

func TestMetadataRefreshRouteFailure(t *testing.T) {
	cache := newTestMetadataCache(t)

	refresher := newTestMetadataRefresher(
		t, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return nil, errors.New("unavailable")
		},
	)

	shard, err := refresher.Route(context.Background(), "alpha")

	if shard != nil || !errors.Is(err, ErrMetadataRefresh) {
		t.Fatalf("shard=%v err=%v", shard, err)
	}
}

func TestMetadataRefreshNilConnectionManager(t *testing.T) {
	refresher, err := NewGRPCMetadataRefresher(
		newTestMetadataCache(t),
		nil,
		[]string{refreshTestEndpoint},
		time.Second,
	)

	if refresher != nil ||
		!errors.Is(err, ErrInvalidRefreshConfig) {
		t.Fatalf("refresher=%v err=%v", refresher, err)
	}
}

func BenchmarkMetadataRefreshCachedEnsure(b *testing.B) {
	cache := newTestMetadataCache(b)
	requireMetadataUpdate(b, cache, 1, 1)

	refresher := newTestMetadataRefresher(
		b, cache, []string{refreshTestEndpoint},
		func(context.Context, string) (*shardraftpb.ShardMap, error) {
			return testShardMap(), nil
		},
	)

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := refresher.Ensure(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
