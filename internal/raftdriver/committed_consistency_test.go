package raftdriver

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// checkCommittedLogConsistency compares committed entries
// across all nodes, including nodes that are currently offline.
//
// Entries compacted into snapshots cannot be compared by
// content. When a retained entry overlaps another node's
// snapshot boundary, their terms must agree.
func checkCommittedLogConsistency(
	t *testing.T,
	c *recoveryTestCluster,
) {
	t.Helper()

	for i := 0; i < len(c.ids); i++ {
		for j := i + 1; j < len(c.ids); j++ {
			idA := c.ids[i]
			idB := c.ids[j]

			a := c.nodes[idA].node
			b := c.nodes[idB].node

			commitA := a.Status().Commit
			commitB := b.Status().Commit

			limit := commitA
			if commitB < limit {
				limit = commitB
			}

			snapshotA := a.SnapshotMetadata()
			snapshotB := b.SnapshotMetadata()

			for index := uint64(1); index <= limit; index++ {
				entryA, okA := a.CommittedEntry(index)
				entryB, okB := b.CommittedEntry(index)

				if okA && okB {
					if !sameCommittedEntry(entryA, entryB) {
						t.Fatalf(
							"committed log conflict: nodes %d and %d at index %d: "+
								"A=(term=%d type=%d data=%q), "+
								"B=(term=%d type=%d data=%q)",
							idA, idB, index,
							entryA.Term, entryA.Type, entryA.Data,
							entryB.Term, entryB.Type, entryB.Data,
						)
					}
					continue
				}

				// If a retained entry is at the other node's
				// snapshot boundary, compare its term.
				if okA && index == snapshotB.LastIncludedIndex {
					if entryA.Term != snapshotB.LastIncludedTerm {
						t.Fatalf(
							"snapshot boundary conflict: nodes %d and %d at index %d",
							idA, idB, index,
						)
					}
				}

				if okB && index == snapshotA.LastIncludedIndex {
					if entryB.Term != snapshotA.LastIncludedTerm {
						t.Fatalf(
							"snapshot boundary conflict: nodes %d and %d at index %d",
							idA, idB, index,
						)
					}
				}
			}
		}
	}
}

func sameCommittedEntry(a, b raft.Entry) bool {
	return a.Index == b.Index &&
		a.Term == b.Term &&
		a.Type == b.Type &&
		bytes.Equal(a.Data, b.Data)
}

func TestCommittedEntryReadOnly(t *testing.T) {
	r := raft.NewRaft(1, nil, 5)

	for i := 0; i < 10; i++ {
		r.Tick()
	}

	if r.Status().Role != raft.Leader {
		t.Fatal("single node did not become leader")
	}

	if err := r.Propose([]byte("original")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	index := r.Status().Commit
	entry, ok := r.CommittedEntry(index)
	if !ok {
		t.Fatalf("committed entry %d unavailable", index)
	}

	if len(entry.Data) == 0 {
		t.Fatal("expected nonempty command data")
	}

	entry.Data[0] = 'X'

	again, ok := r.CommittedEntry(index)
	if !ok {
		t.Fatal("committed entry disappeared")
	}

	if string(again.Data) != "original" {
		t.Fatalf(
			"committed entry was mutated: %q",
			again.Data,
		)
	}
}

func TestCommittedLogConsistencyAfterRecovery(t *testing.T) {
	c := newRecoveryTestCluster(t)

	c.advance(20)

	leader := connectedMajorityLeader(c)
	if leader == 0 {
		t.Fatal("no connected majority leader")
	}

	for i := 0; i < 5; i++ {
		c.propose(
			leader,
			fmt.Sprintf("consistency-%d", i),
			fmt.Sprintf("value-%d", i),
		)
	}

	c.advance(20)
	checkCommittedLogConsistency(t, c)

	// Crash and restart one follower.
	for _, id := range c.ids {
		if id != leader {
			c.crash(id)
			c.advance(5)
			c.restart(id)
			break
		}
	}

	c.advance(30)
	checkCommittedLogConsistency(t, c)
}
