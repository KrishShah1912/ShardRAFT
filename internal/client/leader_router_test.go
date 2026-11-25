package client

import (
	"errors"
	"sync"
	"testing"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

func testLeaderShard() *shardraftpb.ShardInfo {
	return testShardMap().GetShards()[0]
}

func testLeaderHint(
	leaderID uint64,
	term uint64,
	epoch uint64,
) *shardraftpb.RoutingError {
	return &shardraftpb.RoutingError{
		Code:     shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
		ShardId:  1,
		Epoch:    epoch,
		Term:     term,
		LeaderId: leaderID,
	}
}

func requirePreferredReplica(
	t testing.TB,
	router *LeaderRouter,
	shard *shardraftpb.ShardInfo,
	expected uint64,
) {
	t.Helper()

	replica, err := router.Preferred(shard)

	if err != nil || replica.NodeID != expected {
		t.Fatalf(
			"replica=%+v expected=%d err=%v",
			replica,
			expected,
			err,
		)
	}
}

func TestLeaderRouterMetadataLeader(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()
	shard.LeaderId = 3

	requirePreferredReplica(t, router, shard, 3)

	candidates, err := router.Candidates(shard)

	if err != nil {
		t.Fatal(err)
	}

	expected := []uint64{3, 1, 2}

	if len(candidates) != len(expected) {
		t.Fatal("unexpected candidate count")
	}

	for i, id := range expected {
		if candidates[i].NodeID != id {
			t.Fatal("incorrect candidate ordering")
		}
	}
}

func TestLeaderRouterUnknownLeader(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()
	shard.LeaderId = 0

	requirePreferredReplica(t, router, shard, 1)
}

func TestLeaderRouterMissingLeaderEndpoint(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()
	shard.Replicas[0].Address = ""

	requirePreferredReplica(t, router, shard, 2)
}

func TestLeaderRouterInvalidEndpoints(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	shard.Replicas[0].Address = "0.0.0.0:9091"
	shard.Replicas[1].Address = "localhost:0"

	requirePreferredReplica(t, router, shard, 3)
}

func TestLeaderRouterNoEndpoints(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	for _, replica := range shard.Replicas {
		replica.Address = ""
	}

	_, err := router.Preferred(shard)

	if !errors.Is(err, ErrNoReplicaEndpoint) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLeaderRouterInvalidShard(t *testing.T) {
	router := NewLeaderRouter()

	for _, shard := range []*shardraftpb.ShardInfo{
		nil,
		{},
	} {
		_, err := router.Candidates(shard)

		if !errors.Is(err, ErrInvalidShardMap) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestLeaderRouterObserveHint(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	err := router.ObserveHint(
		shard,
		testLeaderHint(2, 5, 1),
	)

	if err != nil {
		t.Fatal(err)
	}

	requirePreferredReplica(t, router, shard, 2)
}

func TestLeaderRouterNewerTerm(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	for _, hint := range []*shardraftpb.RoutingError{
		testLeaderHint(2, 5, 1),
		testLeaderHint(3, 6, 1),
	} {
		if err := router.ObserveHint(shard, hint); err != nil {
			t.Fatal(err)
		}
	}

	requirePreferredReplica(t, router, shard, 3)
}

func TestLeaderRouterRejectsOlderTerm(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	if err := router.ObserveHint(
		shard, testLeaderHint(2, 5, 1),
	); err != nil {
		t.Fatal(err)
	}

	err := router.ObserveHint(
		shard, testLeaderHint(3, 4, 1),
	)

	if !errors.Is(err, ErrStaleLeaderHint) {
		t.Fatalf("unexpected error: %v", err)
	}

	requirePreferredReplica(t, router, shard, 2)
}

func TestLeaderRouterConflictingSameTerm(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	if err := router.ObserveHint(
		shard, testLeaderHint(2, 5, 1),
	); err != nil {
		t.Fatal(err)
	}

	err := router.ObserveHint(
		shard, testLeaderHint(3, 5, 1),
	)

	if !errors.Is(err, ErrInvalidLeaderHint) {
		t.Fatalf("unexpected error: %v", err)
	}

	requirePreferredReplica(t, router, shard, 2)
}

func TestLeaderRouterRepeatedHint(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	for i := 0; i < 20; i++ {
		if err := router.ObserveHint(
			shard, testLeaderHint(2, 5, 1),
		); err != nil {
			t.Fatal(err)
		}
	}

	requirePreferredReplica(t, router, shard, 2)
}

func TestLeaderRouterInvalidHints(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*shardraftpb.RoutingError)
	}{
		{
			"UnknownLeader",
			func(h *shardraftpb.RoutingError) {
				h.LeaderId = 999
			},
		},
		{
			"ZeroLeader",
			func(h *shardraftpb.RoutingError) {
				h.LeaderId = 0
			},
		},
		{
			"WrongShard",
			func(h *shardraftpb.RoutingError) {
				h.ShardId = 999
			},
		},
		{
			"WrongEpoch",
			func(h *shardraftpb.RoutingError) {
				h.Epoch = 2
			},
		},
		{
			"ZeroTerm",
			func(h *shardraftpb.RoutingError) {
				h.Term = 0
			},
		},
		{
			"WrongErrorCode",
			func(h *shardraftpb.RoutingError) {
				h.Code =
					shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := NewLeaderRouter()
			shard := testLeaderShard()
			hint := testLeaderHint(2, 5, 1)

			tt.mutate(hint)

			err := router.ObserveHint(shard, hint)

			if !errors.Is(err, ErrInvalidLeaderHint) {
				t.Fatalf("unexpected error: %v", err)
			}

			requirePreferredReplica(t, router, shard, 1)
		})
	}
}

func TestLeaderRouterNilHint(t *testing.T) {
	router := NewLeaderRouter()

	err := router.ObserveHint(testLeaderShard(), nil)

	if !errors.Is(err, ErrInvalidLeaderHint) {
		t.Fatalf("unexpected error: %v", err)
	}

	err = router.ObserveHint(nil, testLeaderHint(2, 5, 1))

	if !errors.Is(err, ErrInvalidLeaderHint) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLeaderRouterIgnoresUntrustedAddress(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	hint := testLeaderHint(2, 5, 1)
	hint.LeaderAddress = "untrusted.example.com:9092"

	if err := router.ObserveHint(shard, hint); err != nil {
		t.Fatal(err)
	}

	replica, err := router.Preferred(shard)

	if err != nil {
		t.Fatal(err)
	}

	if replica.Address != "127.0.0.1:9092" {
		t.Fatal("routing hint replaced metadata address")
	}
}

func TestLeaderRouterEpochChange(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	if err := router.ObserveHint(
		shard, testLeaderHint(3, 5, 1),
	); err != nil {
		t.Fatal(err)
	}

	shard.Epoch = 2
	shard.LeaderId = 2

	requirePreferredReplica(t, router, shard, 2)

	if err := router.ObserveHint(
		shard, testLeaderHint(1, 6, 2),
	); err != nil {
		t.Fatal(err)
	}

	requirePreferredReplica(t, router, shard, 1)

	err := router.ObserveHint(
		testLeaderShard(),
		testLeaderHint(3, 7, 1),
	)

	if !errors.Is(err, ErrStaleLeaderHint) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLeaderRouterInvalidation(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	if err := router.ObserveHint(
		shard, testLeaderHint(3, 5, 1),
	); err != nil {
		t.Fatal(err)
	}

	router.Invalidate(1, 2)
	requirePreferredReplica(t, router, shard, 3)

	router.Invalidate(1, 3)
	requirePreferredReplica(t, router, shard, 1)

	err := router.ObserveHint(
		shard, testLeaderHint(2, 4, 1),
	)

	if !errors.Is(err, ErrStaleLeaderHint) {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := router.ObserveHint(
		shard, testLeaderHint(3, 5, 1),
	); err != nil {
		t.Fatal(err)
	}

	requirePreferredReplica(t, router, shard, 3)
}

func TestLeaderRouterConcurrentAccess(t *testing.T) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	const (
		readers = 32
		lookups = 1000
		updates = 100
	)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < readers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			for j := 0; j < lookups; j++ {
				candidates, err := router.Candidates(shard)

				if err != nil || len(candidates) != 3 {
					t.Errorf("invalid candidates: %v", err)
					return
				}

				seen := make(map[uint64]bool)

				for _, replica := range candidates {
					if seen[replica.NodeID] {
						t.Error("duplicate candidate")
						return
					}

					seen[replica.NodeID] = true
				}
			}
		}()
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		<-start

		for i := 1; i <= updates; i++ {
			hint := testLeaderHint(
				uint64(i%3+1),
				uint64(i),
				1,
			)

			if err := router.ObserveHint(
				shard, hint,
			); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	close(start)
	wg.Wait()

	requirePreferredReplica(
		t, router, shard, uint64(updates%3+1),
	)

	t.Logf(
		"validated %d concurrent lookups and %d leader updates",
		readers*lookups,
		updates,
	)
}

func BenchmarkLeaderRouterPreferred(b *testing.B) {
	router := NewLeaderRouter()
	shard := testLeaderShard()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = router.Preferred(shard)
	}
}
