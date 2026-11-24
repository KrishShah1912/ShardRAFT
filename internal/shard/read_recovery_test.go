package shard

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

type linearizableRecoveryFixture struct {
	t    *testing.T
	path string
	s    *Shard
}

func newLinearizableRecoveryFixture(
	t *testing.T,
) *linearizableRecoveryFixture {
	t.Helper()

	f := &linearizableRecoveryFixture{
		t:    t,
		path: filepath.Join(t.TempDir(), "node.wal"),
	}

	f.s = openTestShard(t, f.path)

	t.Cleanup(func() {
		if err := f.s.Close(); err != nil {
			t.Error(err)
		}
	})

	electTestLeader(t, f.s)

	return f
}

func (f *linearizableRecoveryFixture) restart() {
	f.t.Helper()

	if err := f.s.Close(); err != nil {
		f.t.Fatal(err)
	}

	f.s = openTestShard(f.t, f.path)

	requireLinearizableNoPending(f.t, f.s)

	ctx, cancel := context.WithTimeout(
		context.Background(), time.Second,
	)
	defer cancel()

	value, found, err := f.s.GetLinearizable(
		ctx, "alpha",
	)

	if !errors.Is(err, raft.ErrNotLeader) ||
		value != "" || found {
		f.t.Fatalf(
			"recovered follower returned (%q, %v, %v)",
			value, found, err,
		)
	}

	electTestLeader(f.t, f.s)
}

func (f *linearizableRecoveryFixture) status() raft.Status {
	f.t.Helper()

	status, _, err := f.s.StatusSnapshot()

	if err != nil {
		f.t.Fatal(err)
	}

	return status
}

func TestShardLinearizableRecoveryWALRestart(t *testing.T) {
	f := newLinearizableRecoveryFixture(t)

	putLinearizableTestValue(t, f.s, "alpha", "100")
	putLinearizableTestValue(t, f.s, "alpha", "200")
	putLinearizableTestValue(t, f.s, "beta", "300")

	if err := f.s.Propose(kvstore.Command{
		Op:  kvstore.OpDelete,
		Key: "beta",
	}); err != nil {
		t.Fatal(err)
	}

	requireLinearizableTestValue(
		t, f.s, "alpha", "200", true,
	)

	before := f.status()

	f.restart()

	after := f.status()

	if after.Term <= before.Term ||
		after.Commit <= before.Commit {
		t.Fatal("fresh leadership barrier not established")
	}

	requireLinearizableTestValue(
		t, f.s, "alpha", "200", true,
	)

	requireLinearizableTestValue(
		t, f.s, "beta", "", false,
	)

	requireLinearizableNoPending(t, f.s)
}

func TestShardLinearizableRecoveryCompactedRestart(t *testing.T) {
	f := newLinearizableRecoveryFixture(t)

	putLinearizableTestValue(t, f.s, "alpha", "100")
	putLinearizableTestValue(t, f.s, "beta", "200")

	requireLinearizableTestValue(
		t, f.s, "alpha", "100", true,
	)

	if err := f.s.Compact(f.s.snapshotPath); err != nil {
		t.Fatal(err)
	}

	// Validate reads at the compacted boundary.
	requireLinearizableTestValue(
		t, f.s, "alpha", "100", true,
	)

	// Add a retained WAL suffix after compaction.
	putLinearizableTestValue(t, f.s, "alpha", "300")
	putLinearizableTestValue(t, f.s, "gamma", "400")

	before := f.status()

	f.restart()

	after := f.status()

	if after.Term <= before.Term ||
		after.Commit <= before.Commit {
		t.Fatal("recovered barrier did not advance")
	}

	requireLinearizableTestValue(
		t, f.s, "alpha", "300", true,
	)

	requireLinearizableTestValue(
		t, f.s, "beta", "200", true,
	)

	requireLinearizableTestValue(
		t, f.s, "gamma", "400", true,
	)

	requireLinearizableNoPending(t, f.s)
}

func TestShardLinearizableRecoveryRepeatedRestart(t *testing.T) {
	f := newLinearizableRecoveryFixture(t)

	previous := f.status()

	for i := 0; i < 5; i++ {
		expected := fmt.Sprintf("value-%d", i)

		putLinearizableTestValue(
			t, f.s, "alpha", expected,
		)

		requireLinearizableTestValue(
			t, f.s, "alpha", expected, true,
		)

		f.restart()

		current := f.status()

		if current.Term <= previous.Term ||
			current.Commit <= previous.Commit {
			t.Fatal("leadership barrier did not advance")
		}

		requireLinearizableTestValue(
			t, f.s, "alpha", expected, true,
		)

		requireLinearizableNoPending(t, f.s)

		previous = current
	}
}

func TestShardLinearizableRecoveryConfirmationDiscard(t *testing.T) {
	f := newLinearizableRecoveryFixture(t)

	putLinearizableTestValue(t, f.s, "alpha", "100")

	oldShard := f.s

	request, err := oldShard.beginLinearizableRead(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}

	var oldState raft.ReadState

	select {
	case result := <-request.done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		oldState = result.state
	default:
		t.Fatal("expected completed confirmation")
	}

	if oldState.RequestID == 0 {
		t.Fatal("invalid old confirmation")
	}

	f.restart()

	if f.status().Term <= oldState.Term {
		t.Fatal("expected fresh leadership term")
	}

	// Deliver an obsolete confirmation to the recovered
	// shard. It must not register or complete any read.
	f.s.mu.Lock()
	err = f.s.handleReadState(oldState)
	f.s.mu.Unlock()

	if err != nil {
		t.Fatal(err)
	}

	requireLinearizableNoPending(t, f.s)

	// The old request must receive a terminal error.
	select {
	case result := <-request.done:
		if !errors.Is(result.err, ErrClosed) {
			t.Fatalf("incorrect terminal error: %v", result.err)
		}
	default:
		t.Fatal("old request was not terminated")
	}

	putLinearizableTestValue(t, f.s, "alpha", "200")

	requireLinearizableTestValue(
		t, f.s, "alpha", "200", true,
	)

	requireLinearizableNoPending(t, f.s)
}
