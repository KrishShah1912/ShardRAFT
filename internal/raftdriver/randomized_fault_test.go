package raftdriver

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// TestRandomizedRaftFaults exercises deterministic
// randomized crashes, restarts, partitions, and proposals.
func TestRandomizedRaftFaults(t *testing.T) {
	runRandomizedRaftFaults(t, 25, 40)
}

func runRandomizedRaftFaults(t *testing.T, seeds, steps int) {

	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			c := newRecoveryTestCluster(t)

			// Allow the initial election to begin.
			c.advance(6)

			confirmed := make(map[string]string)

			for step := 0; step < steps; step++ {
				action := rng.Intn(5)
				id := c.ids[rng.Intn(len(c.ids))]

				switch action {
				case 0:
					// Isolate an online node.
					if c.online[id] {
						c.blocked[id] = true
					}

				case 1:
					// Heal a network partition.
					delete(c.blocked, id)

				case 2:
					// Crash a node, keeping at least
					// two nodes online.
					online := 0

					for _, nodeID := range c.ids {
						if c.online[nodeID] {
							online++
						}
					}

					if online > 2 && c.online[id] {
						c.crash(id)
					}

				case 3:
					// Restart a crashed node.
					if !c.online[id] {
						c.restart(id)
					}

				case 4:
					// Propose only when a leader
					// can reach a majority.
					leader := connectedMajorityLeader(c)

					if leader != 0 {
						key := fmt.Sprintf(
							"seed-%d-step-%d",
							seed,
							step,
						)

						value := fmt.Sprintf(
							"value-%d",
							step,
						)

						c.propose(leader, key, value)

						// Record only applied commands.
						got, ok := c.nodes[leader].store.Get(key)

						if ok && got == value {
							confirmed[key] = value
						}
					}
				}

				c.advance(2)

				// Check committed-log agreement across nodes,
				// including nodes that are temporarily offline.
				checkCommittedLogConsistency(t, c)

				// Safety check: applied index must
				// never exceed committed index.
				for _, nodeID := range c.ids {
					if !c.online[nodeID] {
						continue
					}

					// Validate internal Raft safety invariants.
					if err := c.nodes[nodeID].node.CheckInvariants(); err != nil {
						t.Fatalf(
							"seed=%d step=%d node=%d: invariant violation: %v",
							seed,
							step,
							nodeID,
							err,
						)
					}

					status := c.nodes[nodeID].node.Status()
					applied := c.nodes[nodeID].store.AppliedIndex()

					if applied > status.Commit {
						t.Fatalf(
							"seed=%d step=%d node=%d: applied=%d > commit=%d",
							seed,
							step,
							nodeID,
							applied,
							status.Commit,
						)
					}
				}
			}

			// Restore all network connections and
			// restart every crashed node.
			for _, id := range c.ids {
				delete(c.blocked, id)

				if !c.online[id] {
					c.restart(id)
				}
			}

			// Wait for a stable, fully connected leader.
			var leader raft.NodeID
			var previousLeader raft.NodeID
			stableTicks := 0

			for tick := 0; tick < 200; tick++ {
				c.advance(1)

				var currentLeader raft.NodeID
				leaderCount := 0

				for _, id := range c.ids {
					status := c.nodes[id].node.Status()

					if status.Role == raft.Leader {
						currentLeader = id
						leaderCount++
					}
				}

				if leaderCount == 1 &&
					currentLeader == previousLeader {
					stableTicks++
				} else {
					stableTicks = 0
				}

				if leaderCount == 1 {
					previousLeader = currentLeader
				} else {
					previousLeader = 0
				}

				if stableTicks >= 10 {
					leader = currentLeader

					t.Logf(
						"seed=%d: stable leader=%d after %d ticks",
						seed,
						leader,
						tick+1,
					)
					break
				}
			}

			if leader == 0 {
				for _, id := range c.ids {
					status := c.nodes[id].node.Status()

					t.Logf(
						"seed=%d node=%d role=%v term=%d leader=%d commit=%d",
						seed,
						id,
						status.Role,
						status.Term,
						status.LeaderID,
						status.Commit,
					)
				}

				t.Fatalf(
					"seed=%d: no stable leader after 200 ticks",
					seed,
				)
			}

			// Propose a final recovery command.
			finalKey := fmt.Sprintf("final-%d", seed)

			c.propose(leader, finalKey, "recovered")

			// Wait for the final command to be
			// applied by every node.
			replicated := false

			for tick := 0; tick < 100; tick++ {
				c.advance(1)

				allApplied := true

				for _, id := range c.ids {
					value, exists := c.nodes[id].store.Get(finalKey)

					if !exists || value != "recovered" {
						allApplied = false
						break
					}
				}

				if allApplied {
					replicated = true
					break
				}
			}

			if !replicated {
				for _, id := range c.ids {
					status := c.nodes[id].node.Status()
					value, exists := c.nodes[id].store.Get(finalKey)

					t.Logf(
						"seed=%d node=%d role=%v term=%d commit=%d finalExists=%v finalValue=%q",
						seed,
						id,
						status.Role,
						status.Term,
						status.Commit,
						exists,
						value,
					)
				}

				t.Fatalf(
					"seed=%d: final command did not replicate after recovery",
					seed,
				)
			}

			// Verify all previously confirmed commands.
			for key, expected := range confirmed {
				for _, id := range c.ids {
					c.check(id, key, expected)
				}
			}

			// Verify the final recovery command.
			for _, id := range c.ids {
				c.check(id, finalKey, "recovered")
			}

			// Verify committed and applied indexes.
			c.checkIndexes()

			t.Logf(
				"seed=%d passed; confirmed=%d",
				seed,
				len(confirmed),
			)
		})
	}
}

// connectedMajorityLeader returns a leader only
// when it can communicate with a majority.
func connectedMajorityLeader(
	c *recoveryTestCluster,
) raft.NodeID {
	var leader raft.NodeID

	for _, id := range c.ids {
		if !c.online[id] || c.blocked[id] {
			continue
		}

		if c.nodes[id].node.Status().Role != raft.Leader {
			continue
		}

		reachable := 0

		for _, peer := range c.ids {
			if c.online[peer] && !c.blocked[peer] {
				reachable++
			}
		}

		if reachable < 2 {
			continue
		}

		if leader != 0 {
			return 0
		}

		leader = id
	}

	return leader
}
