package client

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestMetadataCache(
	t testing.TB,
) *MetadataCache {
	t.Helper()

	cache, err := NewMetadataCache(time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	return cache
}

func requireMetadataUpdate(
	t testing.TB,
	cache *MetadataCache,
	epoch uint64,
	leader uint64,
) {
	t.Helper()

	metadata := testShardMap()
	metadata.Epoch = epoch
	metadata.Shards[0].Epoch = epoch
	metadata.Shards[0].LeaderId = leader

	if err := cache.Update(metadata); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataCacheInvalidTTL(t *testing.T) {
	for _, ttl := range []time.Duration{
		0,
		-time.Second,
		-time.Minute,
	} {
		cache, err := NewMetadataCache(ttl)

		if cache != nil ||
			!errors.Is(err, ErrInvalidMetadataTTL) {
			t.Fatalf(
				"ttl=%v cache=%v err=%v",
				ttl,
				cache,
				err,
			)
		}
	}
}

func TestMetadataCacheEmpty(t *testing.T) {
	cache := newTestMetadataCache(t)

	if cache.Epoch() != 0 {
		t.Fatal("empty cache has nonzero epoch")
	}

	if !cache.NeedsRefresh() {
		t.Fatal("empty cache must require refresh")
	}

	if !cache.UpdatedAt().IsZero() {
		t.Fatal("empty cache has update timestamp")
	}

	table, err := cache.Snapshot()

	if table != nil ||
		!errors.Is(err, ErrMetadataUnavailable) {
		t.Fatalf(
			"table=%v err=%v",
			table,
			err,
		)
	}

	shard, err := cache.Route("alpha")

	if shard != nil ||
		!errors.Is(err, ErrMetadataUnavailable) {
		t.Fatalf(
			"shard=%v err=%v",
			shard,
			err,
		)
	}
}

func TestMetadataCacheInitialUpdate(t *testing.T) {
	cache := newTestMetadataCache(t)

	before := time.Now()

	requireMetadataUpdate(t, cache, 1, 1)

	if cache.Epoch() != 1 {
		t.Fatal("unexpected epoch")
	}

	if cache.NeedsRefresh() {
		t.Fatal("fresh metadata requires refresh")
	}

	if cache.UpdatedAt().Before(before) ||
		cache.UpdatedAt().After(time.Now()) {
		t.Fatal("invalid update timestamp")
	}

	table, err := cache.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	if table.ShardCount() != 1 ||
		table.Epoch() != 1 {
		t.Fatal("unexpected routing table")
	}

	shard, err := cache.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if shard.GetShardId() != 1 ||
		shard.GetLeaderId() != 1 {
		t.Fatal("unexpected route")
	}
}

func TestMetadataCacheNewerEpoch(t *testing.T) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 1, 1)
	requireMetadataUpdate(t, cache, 2, 2)

	if cache.Epoch() != 2 {
		t.Fatal("newer epoch was not published")
	}

	shard, err := cache.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if shard.GetEpoch() != 2 ||
		shard.GetLeaderId() != 2 {
		t.Fatal("new metadata was not published")
	}
}

func TestMetadataCacheRejectsOlderEpoch(t *testing.T) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 3, 2)

	before := cache.UpdatedAt()

	metadata := testShardMap()

	err := cache.Update(metadata)

	if !errors.Is(err, ErrStaleMetadata) {
		t.Fatalf("unexpected error: %v", err)
	}

	if cache.Epoch() != 3 {
		t.Fatal("routing epoch regressed")
	}

	if !cache.UpdatedAt().Equal(before) {
		t.Fatal("rejected update changed timestamp")
	}

	shard, err := cache.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if shard.GetLeaderId() != 2 {
		t.Fatal("rejected update changed leader")
	}
}

func TestMetadataCacheSameEpochLeaderChange(t *testing.T) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 1, 1)
	requireMetadataUpdate(t, cache, 1, 2)

	shard, err := cache.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if cache.Epoch() != 1 ||
		shard.GetLeaderId() != 2 {
		t.Fatal("same-epoch leader update failed")
	}
}

func TestMetadataCacheUnknownLeader(t *testing.T) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 1, 1)
	requireMetadataUpdate(t, cache, 1, 0)

	shard, err := cache.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if shard.GetLeaderId() != 0 {
		t.Fatal("unknown leader was not preserved")
	}
}

func TestMetadataCacheInvalidUpdatePreservesSnapshot(
	t *testing.T,
) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 1, 1)

	before, err := cache.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	updatedAt := cache.UpdatedAt()

	metadata := testShardMap()
	metadata.Epoch = 2
	metadata.Shards[0].SlotRanges[0].EndSlot = 100

	err = cache.Update(metadata)

	if !errors.Is(err, ErrInvalidShardMap) {
		t.Fatalf("unexpected error: %v", err)
	}

	after, err := cache.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	if before != after ||
		cache.Epoch() != 1 ||
		!cache.UpdatedAt().Equal(updatedAt) {
		t.Fatal("invalid update changed cache")
	}
}

func TestMetadataCacheInvalidInitialUpdate(t *testing.T) {
	cache := newTestMetadataCache(t)

	err := cache.Update(nil)

	if !errors.Is(err, ErrInvalidShardMap) {
		t.Fatalf("unexpected error: %v", err)
	}

	if cache.Epoch() != 0 ||
		!cache.NeedsRefresh() ||
		!cache.UpdatedAt().IsZero() {
		t.Fatal("invalid update initialized cache")
	}
}

func TestMetadataCacheExpiration(t *testing.T) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 1, 1)

	// Set the timestamp directly to avoid timing-dependent
	// sleeps and make expiration testing deterministic.
	cache.mu.Lock()
	cache.updatedAt = time.Now().Add(-2 * time.Minute)
	cache.mu.Unlock()

	if !cache.NeedsRefresh() {
		t.Fatal("expired metadata requires refresh")
	}

	if cache.Epoch() != 1 {
		t.Fatal("expiration discarded routing epoch")
	}

	if _, err := cache.Snapshot(); err != nil {
		t.Fatal("expiration discarded snapshot")
	}

	requireMetadataUpdate(t, cache, 1, 1)

	if cache.NeedsRefresh() {
		t.Fatal("successful update did not renew cache")
	}
}

func TestMetadataCacheInvalidation(t *testing.T) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 2, 1)

	cache.Invalidate()

	if !cache.NeedsRefresh() {
		t.Fatal("invalidated metadata requires refresh")
	}

	if !cache.UpdatedAt().IsZero() {
		t.Fatal("invalidation retained timestamp")
	}

	if cache.Epoch() != 2 {
		t.Fatal("invalidation discarded epoch")
	}

	shard, err := cache.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if shard.GetShardId() != 1 {
		t.Fatal("invalidation discarded snapshot")
	}

	requireMetadataUpdate(t, cache, 2, 2)

	if cache.NeedsRefresh() {
		t.Fatal("refresh did not clear invalidation")
	}
}

func TestMetadataCacheInvalidationRejectsOlderEpoch(
	t *testing.T,
) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 5, 1)

	cache.Invalidate()

	err := cache.Update(testShardMap())

	if !errors.Is(err, ErrStaleMetadata) {
		t.Fatalf("unexpected error: %v", err)
	}

	if cache.Epoch() != 5 ||
		!cache.NeedsRefresh() {
		t.Fatal("stale update changed invalidated cache")
	}
}

func TestMetadataCacheSnapshotIsolation(t *testing.T) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 1, 1)

	old, err := cache.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	requireMetadataUpdate(t, cache, 2, 2)

	current, err := cache.Snapshot()

	if err != nil {
		t.Fatal(err)
	}

	oldShard, err := old.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	newShard, err := current.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if old.Epoch() != 1 ||
		oldShard.GetLeaderId() != 1 ||
		current.Epoch() != 2 ||
		newShard.GetLeaderId() != 2 {
		t.Fatal("snapshot isolation failed")
	}
}

func TestMetadataCacheInputIsolation(t *testing.T) {
	cache := newTestMetadataCache(t)

	metadata := testShardMap()

	if err := cache.Update(metadata); err != nil {
		t.Fatal(err)
	}

	metadata.Epoch = 999
	metadata.Shards[0].LeaderId = 999
	metadata.Shards[0].Replicas[0].Address = "changed"

	shard, err := cache.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	if cache.Epoch() != 1 ||
		shard.GetLeaderId() != 1 ||
		shard.GetReplicas()[0].GetAddress() !=
			"127.0.0.1:9091" {
		t.Fatal("input mutation changed cache")
	}
}

func TestMetadataCacheConcurrentReadersAndWriter(
	t *testing.T,
) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 1, 1)

	const (
		readers = 32
		reads   = 1000
		updates = 100
	)

	var wg sync.WaitGroup

	start := make(chan struct{})

	for worker := 0; worker < readers; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()
			<-start

			var lastEpoch uint64

			for i := 0; i < reads; i++ {
				table, err := cache.Snapshot()

				if err != nil {
					t.Error(err)
					return
				}

				if table.Epoch() < lastEpoch {
					t.Error("observed epoch regression")
					return
				}

				lastEpoch = table.Epoch()

				key := fmt.Sprintf(
					"worker-%d-key-%d",
					worker,
					i,
				)

				shard, err := table.Route(key)

				if err != nil {
					t.Error(err)
					return
				}

				if shard.GetShardId() != 1 ||
					shard.GetEpoch() != table.Epoch() {
					t.Error("inconsistent snapshot")
					return
				}

				shard.LeaderId = 999

				_ = cache.Epoch()
				_ = cache.UpdatedAt()
				_ = cache.NeedsRefresh()
			}
		}(worker)
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < updates; i++ {
			epoch := uint64(i + 2)

			metadata := testShardMap()
			metadata.Epoch = epoch
			metadata.Shards[0].Epoch = epoch
			metadata.Shards[0].LeaderId =
				uint64(i%3 + 1)

			if err := cache.Update(metadata); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	close(start)
	wg.Wait()

	if cache.Epoch() != updates+1 {
		t.Fatalf(
			"final epoch=%d want=%d",
			cache.Epoch(),
			updates+1,
		)
	}

	t.Logf(
		"validated %d concurrent reads and %d updates",
		readers*reads,
		updates,
	)
}

func TestMetadataCacheConcurrentWriters(t *testing.T) {
	cache := newTestMetadataCache(t)

	requireMetadataUpdate(t, cache, 1, 1)

	const writers = 32

	var wg sync.WaitGroup

	start := make(chan struct{})

	for i := 0; i < writers; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			<-start

			epoch := uint64(i + 2)

			metadata := testShardMap()
			metadata.Epoch = epoch
			metadata.Shards[0].Epoch = epoch

			err := cache.Update(metadata)

			if err != nil &&
				!errors.Is(err, ErrStaleMetadata) {
				t.Error(err)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	if cache.Epoch() != writers+1 {
		t.Fatalf(
			"final epoch=%d want=%d",
			cache.Epoch(),
			writers+1,
		)
	}
}

func BenchmarkMetadataCacheSnapshot(b *testing.B) {
	cache := newTestMetadataCache(b)

	requireMetadataUpdate(b, cache, 1, 1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = cache.Snapshot()
	}
}

func BenchmarkMetadataCacheRoute(b *testing.B) {
	cache := newTestMetadataCache(b)

	requireMetadataUpdate(b, cache, 1, 1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = cache.Route("user:123456789")
	}
}

func BenchmarkMetadataCacheParallelSnapshot(b *testing.B) {
	cache := newTestMetadataCache(b)

	requireMetadataUpdate(b, cache, 1, 1)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cache.Snapshot()
		}
	})
}
