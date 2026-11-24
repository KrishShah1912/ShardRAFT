package shard

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func newLinearizableReadFixture(
	t *testing.T,
	elect bool,
) *Shard {
	t.Helper()

	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)

	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})

	if elect {
		electTestLeader(t, s)
	}

	return s
}

func putLinearizableTestValue(
	t *testing.T,
	s *Shard,
	key, value string,
) {
	t.Helper()

	if err := s.Propose(kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   key,
		Value: value,
	}); err != nil {
		t.Fatal(err)
	}
}

func requireLinearizableTestValue(
	t *testing.T,
	s *Shard,
	key, expected string,
	found bool,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	value, exists, err := s.GetLinearizable(ctx, key)

	if err != nil {
		t.Fatal(err)
	}

	if value != expected || exists != found {
		t.Fatalf(
			"key %q: got (%q, %v), want (%q, %v)",
			key, value, exists, expected, found,
		)
	}
}

func requireLinearizableNoPending(
	t *testing.T,
	s *Shard,
) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pendingReadCount() != 0 {
		t.Fatalf(
			"pending reads retained: %d",
			s.pendingReadCount(),
		)
	}

	if len(s.node.Ready().ReadStates) != 0 {
		t.Fatal("unacknowledged confirmations remain")
	}
}

func TestShardLinearizableMissingKey(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	requireLinearizableTestValue(
		t, s, "missing", "", false,
	)

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizablePut(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "alpha", "100")

	requireLinearizableTestValue(
		t, s, "alpha", "100", true,
	)

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableOverwrite(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "alpha", "100")
	putLinearizableTestValue(t, s, "alpha", "200")

	requireLinearizableTestValue(
		t, s, "alpha", "200", true,
	)

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableDelete(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "alpha", "100")

	if err := s.Propose(kvstore.Command{
		Op:  kvstore.OpDelete,
		Key: "alpha",
	}); err != nil {
		t.Fatal(err)
	}

	requireLinearizableTestValue(
		t, s, "alpha", "", false,
	)

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableCAS(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "counter", "10")

	if err := s.Propose(kvstore.Command{
		Op:       kvstore.OpCompareAndSwap,
		Key:      "counter",
		Expected: "10",
		Value:    "20",
	}); err != nil {
		t.Fatal(err)
	}

	requireLinearizableTestValue(
		t, s, "counter", "20", true,
	)

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableEmptyValue(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "empty", "")

	requireLinearizableTestValue(
		t, s, "empty", "", true,
	)

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableFollowerRejects(t *testing.T) {
	s := newLinearizableReadFixture(t, false)

	value, found, err := s.GetLinearizable(
		context.Background(),
		"alpha",
	)

	if value != "" || found ||
		!errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf(
			"unexpected follower result: %q %v %v",
			value, found, err,
		)
	}

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableClosed(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	value, found, err := s.GetLinearizable(
		context.Background(),
		"alpha",
	)

	if value != "" || found ||
		!errors.Is(err, ErrClosed) {
		t.Fatalf("unexpected closed result: %v", err)
	}
}

func TestShardLinearizableStopped(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	value, found, err := s.GetLinearizable(
		context.Background(),
		"alpha",
	)

	if value != "" || found ||
		!errors.Is(err, ErrStopped) {
		t.Fatalf("unexpected stopped result: %v", err)
	}

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableEmptyKey(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	_, _, err := s.GetLinearizable(
		context.Background(),
		"",
	)

	if !errors.Is(err, ErrInvalidReadKey) {
		t.Fatalf("unexpected key error: %v", err)
	}

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableNilContext(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	_, _, err := s.GetLinearizable(nil, "alpha")

	if !errors.Is(err, ErrInvalidReadContext) {
		t.Fatalf("unexpected context error: %v", err)
	}

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableCanceledContext(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, _, err := s.GetLinearizable(ctx, "alpha")

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancellation error: %v", err)
	}

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableExpiredDeadline(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	_, _, err := s.GetLinearizable(ctx, "alpha")

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected deadline error: %v", err)
	}

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableRepeatedReads(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "alpha", "100")

	for i := 0; i < 100; i++ {
		requireLinearizableTestValue(
			t, s, "alpha", "100", true,
		)
	}

	requireLinearizableNoPending(t, s)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.nextReadID != 100 {
		t.Fatalf(
			"unexpected request identity: %d",
			s.nextReadID,
		)
	}
}

func TestShardLinearizableConcurrentReads(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "alpha", "100")
	putLinearizableTestValue(t, s, "beta", "200")

	const readers = 64

	var wg sync.WaitGroup

	results := make(chan error, readers)

	for i := 0; i < readers; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key, expected := "alpha", "100"

			if i%2 == 1 {
				key, expected = "beta", "200"
			}

			ctx, cancel := context.WithTimeout(
				context.Background(),
				3*time.Second,
			)
			defer cancel()

			value, found, err :=
				s.GetLinearizable(ctx, key)

			if err != nil {
				results <- err
				return
			}

			if !found || value != expected {
				results <- fmt.Errorf(
					"reader %d: unexpected value %q",
					i, value,
				)
				return
			}

			results <- nil
		}(i)
	}

	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Error(err)
		}
	}

	requireLinearizableNoPending(t, s)
}

func TestShardLinearizableNoLogGrowth(t *testing.T) {
	s := newLinearizableReadFixture(t, true)

	putLinearizableTestValue(t, s, "alpha", "100")

	s.mu.Lock()

	before := s.node.Status()
	beforeLog := s.node.DebugLog()
	applied := s.store.AppliedIndex()

	s.mu.Unlock()

	for i := 0; i < 100; i++ {
		requireLinearizableTestValue(
			t, s, "alpha", "100", true,
		)
	}

	requireLinearizableNoPending(t, s)

	s.mu.Lock()
	defer s.mu.Unlock()

	after := s.node.Status()

	if before != after {
		t.Fatal("reads modified Raft status")
	}

	if !reflect.DeepEqual(beforeLog, s.node.DebugLog()) {
		t.Fatal("reads modified Raft log")
	}

	if s.store.AppliedIndex() != applied {
		t.Fatal("reads changed application index")
	}
}
