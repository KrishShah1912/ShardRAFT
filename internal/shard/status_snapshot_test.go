package shard

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func requireStatusSnapshot(
	t *testing.T,
	s *Shard,
) (raft.Status, uint64) {
	t.Helper()

	state, applied, err := s.StatusSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	if state.ID != 1 {
		t.Fatalf("unexpected node ID: %d", state.ID)
	}

	if applied > state.Commit {
		t.Fatalf(
			"applied index %d exceeds commit index %d",
			applied, state.Commit,
		)
	}

	return state, applied
}

func proposeSnapshotTestWrite(
	t *testing.T,
	s *Shard,
	index int,
) {
	t.Helper()

	err := s.Propose(kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "snapshot-key",
		Value:     fmt.Sprintf("value-%d", index),
		ClientID:  "snapshot-test-client",
		RequestID: fmt.Sprintf("request-%d", index),
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestStatusSnapshotInitialState(t *testing.T) {
	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)
	defer s.Close()

	state, applied := requireStatusSnapshot(t, s)

	if state.Role != raft.Follower {
		t.Fatalf("initial role = %v, want follower", state.Role)
	}

	if state.LeaderID != 0 {
		t.Fatalf("unexpected initial leader: %d", state.LeaderID)
	}

	if state.Commit != 0 || applied != 0 {
		t.Fatalf(
			"unexpected initial indexes: commit=%d applied=%d",
			state.Commit, applied,
		)
	}
}

func TestStatusSnapshotAfterElection(t *testing.T) {
	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)
	defer s.Close()

	before, _ := requireStatusSnapshot(t, s)

	electTestLeader(t, s)

	after, applied := requireStatusSnapshot(t, s)

	if after.Role != raft.Leader {
		t.Fatalf("role = %v, want leader", after.Role)
	}

	if after.LeaderID != after.ID {
		t.Fatalf("unexpected leader ID: %d", after.LeaderID)
	}

	if after.Term <= before.Term {
		t.Fatalf(
			"term did not advance: %d -> %d",
			before.Term, after.Term,
		)
	}

	if after.Commit == 0 || applied != after.Commit {
		t.Fatalf(
			"leader no-op not fully applied: commit=%d applied=%d",
			after.Commit, applied,
		)
	}
}

func TestStatusSnapshotAfterMutations(t *testing.T) {
	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)
	defer s.Close()

	electTestLeader(t, s)

	before, beforeApplied := requireStatusSnapshot(t, s)

	const writes = 10

	for i := 0; i < writes; i++ {
		proposeSnapshotTestWrite(t, s, i)
	}

	after, afterApplied := requireStatusSnapshot(t, s)

	if after.Commit != before.Commit+writes {
		t.Fatalf(
			"commit index = %d, want %d",
			after.Commit, before.Commit+writes,
		)
	}

	if afterApplied != beforeApplied+writes {
		t.Fatalf(
			"applied index = %d, want %d",
			afterApplied, beforeApplied+writes,
		)
	}

	if afterApplied != after.Commit {
		t.Fatal("committed entries were not fully applied")
	}

	if after.Term != before.Term ||
		after.Role != raft.Leader {
		t.Fatal("unexpected leadership change")
	}

	value, found, err := s.Get("snapshot-key")

	if err != nil || !found || value != "value-9" {
		t.Fatalf(
			"unexpected KV state: value=%q found=%v err=%v",
			value, found, err,
		)
	}
}

func TestStatusSnapshotMatchesExistingAPIs(t *testing.T) {
	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)
	defer s.Close()

	electTestLeader(t, s)
	proposeSnapshotTestWrite(t, s, 1)

	combined, combinedApplied := requireStatusSnapshot(t, s)

	separate, err := s.Status()
	if err != nil {
		t.Fatal(err)
	}

	separateApplied, err := s.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}

	if combined != separate {
		t.Fatalf(
			"status mismatch: combined=%+v separate=%+v",
			combined, separate,
		)
	}

	if combinedApplied != separateApplied {
		t.Fatalf(
			"applied index mismatch: combined=%d separate=%d",
			combinedApplied, separateApplied,
		)
	}
}

func TestStatusSnapshotConcurrentMutations(t *testing.T) {
	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)
	defer s.Close()

	electTestLeader(t, s)

	initial, initialApplied := requireStatusSnapshot(t, s)

	const (
		writes  = 50
		readers = 8
		reads   = 500
	)

	start := make(chan struct{})
	errs := make(chan error, readers+1)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < writes; i++ {
			err := s.Propose(kvstore.Command{
				Op:        kvstore.OpPut,
				Key:       "concurrent-key",
				Value:     fmt.Sprintf("value-%d", i),
				ClientID:  "concurrent-client",
				RequestID: fmt.Sprintf("request-%d", i),
			})

			if err != nil {
				errs <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for reader := 0; reader < readers; reader++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			<-start

			lastCommit := initial.Commit
			lastApplied := initialApplied

			for i := 0; i < reads; i++ {
				state, applied, err := s.StatusSnapshot()

				if err != nil {
					errs <- fmt.Errorf(
						"reader %d: %w", id, err,
					)
					return
				}

				if state.ID != initial.ID ||
					state.Role != raft.Leader ||
					state.LeaderID != initial.ID ||
					state.Term != initial.Term {
					errs <- fmt.Errorf(
						"reader %d: unexpected status %+v",
						id, state,
					)
					return
				}

				if applied > state.Commit {
					errs <- fmt.Errorf(
						"reader %d: applied=%d exceeds commit=%d",
						id, applied, state.Commit,
					)
					return
				}

				if state.Commit < lastCommit ||
					applied < lastApplied {
					errs <- fmt.Errorf(
						"reader %d: indexes moved backwards",
						id,
					)
					return
				}

				lastCommit = state.Commit
				lastApplied = applied
			}
		}(reader)
	}

	close(start)

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	if t.Failed() {
		return
	}

	final, finalApplied := requireStatusSnapshot(t, s)

	if final.Commit != initial.Commit+writes {
		t.Fatalf(
			"final commit=%d, want=%d",
			final.Commit, initial.Commit+writes,
		)
	}

	if finalApplied != initialApplied+writes ||
		finalApplied != final.Commit {
		t.Fatalf(
			"unexpected final indexes: commit=%d applied=%d",
			final.Commit, finalApplied,
		)
	}

	value, found, err := s.Get("concurrent-key")

	if err != nil || !found || value != "value-49" {
		t.Fatalf(
			"unexpected final value=%q found=%v err=%v",
			value, found, err,
		)
	}

	t.Logf(
		"validated %d writes and %d snapshot reads across %d readers",
		writes, readers*reads, readers,
	)
}

func TestStatusSnapshotClosedShard(t *testing.T) {
	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)
	defer s.Close()

	electTestLeader(t, s)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	state, applied, err := s.StatusSnapshot()

	if !errors.Is(err, ErrClosed) {
		t.Fatalf("error=%v, want ErrClosed", err)
	}

	if state != (raft.Status{}) || applied != 0 {
		t.Fatal("closed shard returned nonzero state")
	}
}

func TestStatusSnapshotStoppedShard(t *testing.T) {
	s := openTestShard(
		t,
		filepath.Join(t.TempDir(), "node.wal"),
	)
	defer s.Close()

	electTestLeader(t, s)

	// Follow the existing stopped-shard test pattern.
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	state, applied, err := s.StatusSnapshot()

	if !errors.Is(err, ErrStopped) {
		t.Fatalf("error=%v, want ErrStopped", err)
	}

	if state != (raft.Status{}) || applied != 0 {
		t.Fatal("stopped shard returned nonzero state")
	}
}

func TestStatusSnapshotDurableRecovery(t *testing.T) {
	tests := []struct {
		name    string
		compact bool
	}{
		{"wal_recovery", false},
		{"compacted_snapshot_recovery", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node.wal")

			s := openTestShard(t, path)
			defer s.Close()

			electTestLeader(t, s)

			for i := 0; i < 5; i++ {
				proposeSnapshotTestWrite(t, s, i)
			}

			before, beforeApplied := requireStatusSnapshot(t, s)

			if tc.compact {
				if err := s.Compact(path + ".snapshot"); err != nil {
					t.Fatal(err)
				}

				compacted, compactedApplied := requireStatusSnapshot(t, s)

				if compacted != before ||
					compactedApplied != beforeApplied {
					t.Fatal("compaction changed status indexes")
				}
			}

			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			recovered := openTestShard(t, path)
			defer recovered.Close()

			after, afterApplied := requireStatusSnapshot(t, recovered)

			if after.Commit != before.Commit ||
				afterApplied != beforeApplied {
				t.Fatalf(
					"recovered indexes: commit=%d applied=%d; want commit=%d applied=%d",
					after.Commit, afterApplied,
					before.Commit, beforeApplied,
				)
			}

			if after.Term != before.Term {
				t.Fatalf(
					"recovered term=%d, want=%d",
					after.Term, before.Term,
				)
			}

			if after.Role != raft.Follower ||
				after.LeaderID != 0 {
				t.Fatalf(
					"unexpected recovered leadership: %+v",
					after,
				)
			}

			value, found, err := recovered.Get("snapshot-key")

			if err != nil || !found || value != "value-4" {
				t.Fatalf(
					"recovered value=%q found=%v err=%v",
					value, found, err,
				)
			}

			electTestLeader(t, recovered)

			elected, electedApplied := requireStatusSnapshot(t, recovered)

			if elected.Term <= after.Term ||
				elected.Commit <= after.Commit ||
				electedApplied != elected.Commit {
				t.Fatalf(
					"unexpected post-recovery election: %+v applied=%d",
					elected, electedApplied,
				)
			}

			proposeSnapshotTestWrite(t, recovered, 5)

			final, finalApplied := requireStatusSnapshot(t, recovered)

			if final.Commit != elected.Commit+1 ||
				finalApplied != final.Commit {
				t.Fatal("post-recovery write did not advance indexes")
			}

			value, found, err = recovered.Get("snapshot-key")

			if err != nil || !found || value != "value-5" {
				t.Fatalf(
					"post-recovery value=%q found=%v err=%v",
					value, found, err,
				)
			}
		})
	}
}
