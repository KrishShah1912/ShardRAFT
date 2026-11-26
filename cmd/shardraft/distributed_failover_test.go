package main

import (
	"fmt"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestCLIDistributedBootstrapDiscovery(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	leader := cluster.waitLeader(t, 0)
	before := cluster.waitConvergence(t, 1)

	for _, node := range cluster.nodes {
		if node.id == leader.id {
			continue
		}

		key := fmt.Sprintf("bootstrap-node-%d", node.id)

		cliDistributedMutate(
			t, cluster, node.grpcAddr,
			"bootstrap-discovery",
			fmt.Sprintf("put-%d", node.id),
			cliPut, key, true, "100",
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			key, "100", true, false,
		)

		cliDistributedMutate(
			t, cluster, node.grpcAddr,
			"bootstrap-discovery",
			fmt.Sprintf("cas-%d", node.id),
			cliCAS, key, true, "100", "200",
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			key, "200", true, false,
		)

		cliDistributedMutate(
			t, cluster, node.grpcAddr,
			"bootstrap-discovery",
			fmt.Sprintf("delete-%d", node.id),
			cliDelete, key, true,
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			key, "", false, false,
		)

		t.Logf(
			"Follower-only bootstrap verified: node=%d leader=%d",
			node.id, leader.id,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, before, 6,
	)

	t.Log("Verified discovery through both followers")
	t.Log("Verified PUT, GET, CAS and DELETE")
}

func TestCLIDistributedBootstrapFallback(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	leader := cluster.waitLeader(t, 0)
	unavailable := cluster.follower(t, leader)

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		"bootstrap-fallback", "seed",
		cliPut, "fallback-preserved", true, "stable",
	)

	before := cluster.waitConvergence(t, 2)

	oldPID := unavailable.process.Process.Pid

	unavailable.stop(t)
	cluster.httpClient.CloseIdleConnections()

	if unavailable.process != nil {
		t.Fatal("Unavailable bootstrap node is still running")
	}

	liveFollower := cluster.follower(t, leader)

	endpoints := unavailable.grpcAddr +
		"," + liveFollower.grpcAddr

	t.Logf(
		"Bootstrap endpoints: unavailable=%s available=%s",
		unavailable.grpcAddr,
		liveFollower.grpcAddr,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"fallback-preserved", "stable", true, false,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		"bootstrap-fallback", "put",
		cliPut, "fallback-key", true, "100",
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"fallback-key", "100", true, false,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		"bootstrap-fallback", "cas",
		cliCAS, "fallback-key", true, "100", "200",
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"fallback-key", "200", true, false,
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		"bootstrap-fallback", "delete",
		cliDelete, "fallback-key", true,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"fallback-key", "", false, false,
	)

	unavailable.start(t, cluster.daemonBinary)
	cluster.waitHTTP(t, unavailable)

	if unavailable.process.Process.Pid == oldPID {
		t.Fatal("Expected a new process PID")
	}

	after := cluster.waitConvergence(
		t, before.CommitIndex+3,
	)

	cliDistributedRequireRecoveryProgress(
		t,
		before.Term, before.LeaderID, before.CommitIndex,
		after.Term, after.LeaderID, after.CommitIndex,
		after.AppliedIndex, 3,
	)

	cliDistributedRead(
		t, cluster, unavailable.grpcAddr,
		"fallback-preserved", "stable", true, false,
	)

	cliDistributedRead(
		t, cluster, unavailable.grpcAddr,
		"fallback-key", "", false, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, after, 0,
	)

	for _, node := range cluster.nodes {
		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"fallback-preserved", "stable", true, true,
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"fallback-key", "", false, true,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, after, 0,
	)

	t.Log("Verified local data on all three replicas")
	t.Log("Verified unavailable-first bootstrap fallback")
	t.Log("Verified operation with two running replicas")
	t.Log("Verified recovered bootstrap node")
}

func TestCLIDistributedLeaderFailover(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	oldLeader := cluster.waitLeader(t, 0)

	const clientID = "cli-leader-failover"

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		clientID, "seed-put",
		cliPut, "failover-put", true, "100",
	)

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		clientID, "seed-cas",
		cliPut, "failover-cas", true, "100",
	)

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		clientID, "seed-delete",
		cliPut, "failover-delete", true, "remove-me",
	)

	cliDistributedMutate(
		t, cluster, cluster.endpoints(),
		clientID, "seed-preserved",
		cliPut, "failover-preserved", true, "stable",
	)

	before := cluster.waitConvergence(t, 5)

	oldPID := oldLeader.process.Process.Pid

	survivingPIDs := make(map[int]int)

	for _, node := range cluster.nodes {
		if node != oldLeader {
			survivingPIDs[node.id] =
				node.process.Process.Pid
		}
	}

	oldLeader.stop(t)
	cluster.httpClient.CloseIdleConnections()

	newLeader := cluster.waitLeader(t, oldLeader.id)

	if newLeader.id == oldLeader.id {
		t.Fatal("Leader did not change")
	}

	newStatus, err := cluster.status(newLeader)
	if err != nil {
		t.Fatal(err)
	}

	if newStatus.Term <= before.Term {
		t.Fatalf(
			"Expected higher term: before=%d after=%d",
			before.Term, newStatus.Term,
		)
	}

	if newStatus.CommitIndex < before.CommitIndex {
		t.Fatal("Committed entries were lost")
	}

	t.Logf(
		"Leader failover: node=%d -> %d term=%d -> %d",
		oldLeader.id,
		newLeader.id,
		before.Term,
		newStatus.Term,
	)

	follower := cluster.follower(t, newLeader)

	endpoints := oldLeader.grpcAddr +
		"," + follower.grpcAddr +
		"," + newLeader.grpcAddr

	cliDistributedRead(
		t, cluster, endpoints,
		"failover-put", "100", true, false,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"failover-cas", "100", true, false,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"failover-delete", "remove-me", true, false,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"failover-preserved", "stable", true, false,
	)

	baseline, err := cluster.status(newLeader)
	if err != nil {
		t.Fatal(err)
	}

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "post-failover-put",
		cliPut, "failover-put", true, "200",
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "post-failover-cas",
		cliCAS, "failover-cas", true, "100", "200",
	)

	cliDistributedMutate(
		t, cluster, endpoints,
		clientID, "post-failover-delete",
		cliDelete, "failover-delete", true,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"failover-put", "200", true, false,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"failover-cas", "200", true, false,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"failover-delete", "", false, false,
	)

	cliDistributedRead(
		t, cluster, endpoints,
		"failover-preserved", "stable", true, false,
	)

	committed, err := cluster.status(newLeader)
	if err != nil {
		t.Fatal(err)
	}

	if committed.Term != baseline.Term ||
		committed.LeaderID != uint64(newLeader.id) ||
		committed.CommitIndex != baseline.CommitIndex+3 ||
		committed.AppliedIndex != committed.CommitIndex {
		t.Fatalf(
			"Unexpected post-failover status: before=%+v after=%+v",
			baseline, committed,
		)
	}

	oldLeader.start(t, cluster.daemonBinary)
	cluster.waitHTTP(t, oldLeader)

	if oldLeader.process.Process.Pid == oldPID {
		t.Fatal("Former leader did not restart")
	}

	after := cluster.waitConvergence(
		t, committed.CommitIndex,
	)

	cliDistributedRequireRecoveryProgress(
		t,
		committed.Term, committed.LeaderID,
		committed.CommitIndex,
		after.Term, after.LeaderID, after.CommitIndex,
		after.AppliedIndex, 0,
	)

	recovered, err := cluster.status(oldLeader)
	if err != nil {
		t.Fatal(err)
	}

	if recovered.Term != after.Term ||
		recovered.LeaderID != after.LeaderID ||
		recovered.CommitIndex != after.CommitIndex ||
		recovered.AppliedIndex != after.CommitIndex {
		t.Fatalf(
			"Recovered node did not converge: %+v",
			recovered,
		)
	}

	if after.LeaderID == uint64(oldLeader.id) {
		if recovered.Role != raft.Leader {
			t.Fatalf(
				"Recovered elected leader has invalid role: %+v",
				recovered,
			)
		}
	} else if recovered.Role != raft.Follower {
		t.Fatalf(
			"Recovered node has invalid follower role: %+v",
			recovered,
		)
	}

	for _, node := range cluster.nodes {
		if node == oldLeader {
			continue
		}

		if node.process.Process.Pid != survivingPIDs[node.id] {
			t.Fatalf(
				"Surviving node %d unexpectedly restarted",
				node.id,
			)
		}
	}

	cliDistributedRead(
		t, cluster, oldLeader.grpcAddr,
		"failover-put", "200", true, false,
	)

	cliDistributedRead(
		t, cluster, oldLeader.grpcAddr,
		"failover-cas", "200", true, false,
	)

	cliDistributedRead(
		t, cluster, oldLeader.grpcAddr,
		"failover-delete", "", false, false,
	)

	cliDistributedRead(
		t, cluster, oldLeader.grpcAddr,
		"failover-preserved", "stable", true, false,
	)

	cliDistributedRequireCommitAdvance(
		t, cluster, after, 0,
	)

	for _, node := range cluster.nodes {
		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"failover-put", "200", true, true,
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"failover-cas", "200", true, true,
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"failover-delete", "", false, true,
		)

		cliDistributedRead(
			t, cluster, node.grpcAddr,
			"failover-preserved", "stable", true, true,
		)
	}

	cliDistributedRequireCommitAdvance(
		t, cluster, after, 0,
	)

	t.Log("Verified local data on all three replicas")
	t.Log("Verified leader replacement and term advancement")
	t.Log("Verified committed data preservation")
	t.Log("Verified PUT, CAS and DELETE after failover")
	t.Log("Verified exactly three post-failover mutations")
	t.Log("Verified former leader recovery and convergence")
	t.Log("Verified surviving daemon processes were unchanged")
}

// Recovery can trigger additional elections. Each newly
// elected leader appends one no-op entry.
//
// Require all expected mutations to be committed while
// bounding additional entries by the number of new terms.
func cliDistributedRequireRecoveryProgress(
	t *testing.T,
	beforeTerm uint64,
	beforeLeader uint64,
	beforeCommit uint64,
	afterTerm uint64,
	afterLeader uint64,
	afterCommit uint64,
	afterApplied uint64,
	expectedMutations uint64,
) {
	t.Helper()

	if afterTerm < beforeTerm {
		t.Fatalf(
			"Raft term regressed: before=%d after=%d",
			beforeTerm, afterTerm,
		)
	}

	if afterLeader == 0 {
		t.Fatal("Recovered cluster has no elected leader")
	}

	if afterTerm == beforeTerm &&
		afterLeader != beforeLeader {
		t.Fatalf(
			"Leader changed within the same term: before=%d after=%d term=%d",
			beforeLeader, afterLeader, afterTerm,
		)
	}

	if afterCommit < beforeCommit {
		t.Fatalf(
			"Committed entries were lost: before=%d after=%d",
			beforeCommit, afterCommit,
		)
	}

	if afterApplied != afterCommit {
		t.Fatalf(
			"Recovered state is not fully applied: commit=%d applied=%d",
			afterCommit, afterApplied,
		)
	}

	commitDelta := afterCommit - beforeCommit
	termDelta := afterTerm - beforeTerm

	if commitDelta < expectedMutations {
		t.Fatalf(
			"Missing committed mutations: delta=%d expected=%d",
			commitDelta, expectedMutations,
		)
	}

	extraEntries := commitDelta - expectedMutations

	if extraEntries > termDelta {
		t.Fatalf(
			"Unexpected extra committed entries: extra=%d term_delta=%d",
			extraEntries, termDelta,
		)
	}

	t.Logf(
		"Recovery progress: term=%d -> %d leader=%d -> %d commit=%d -> %d expected_mutations=%d additional_entries=%d",
		beforeTerm, afterTerm,
		beforeLeader, afterLeader,
		beforeCommit, afterCommit,
		expectedMutations, extraEntries,
	)
}
