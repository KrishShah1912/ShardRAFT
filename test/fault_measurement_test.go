package integration

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDockerFaultMeasurements(t *testing.T) {
	requireDocker(t)

	const shard = uint64(1)

	replicas := []uint64{1, 2, 3}

	runID := fmt.Sprintf(
		"fault-measurement-%d",
		time.Now().UnixNano(),
	)

	key := keyForShard(t, runID, uint32(shard))

	leader, before := waitForLeader(
		t, shard, replicas, 0, 1,
	)

	requireAppliedMutation(t, put(
		t, gateway1, key,
		"before-failure", runID, "write-1",
	))

	requireValue(t, gateway2, key, "before-failure")

	t.Logf(
		"Original leader=%d term=%d",
		leader, before.Term,
	)

	failureStarted := time.Now()

	stopNode(t, leader)

	stopDuration := time.Since(failureStarted)
	electionStarted := time.Now()

	newLeader, after := waitForLeader(
		t, shard, replicas, leader, before.Term+1,
	)

	electionDuration := time.Since(electionStarted)

	requireValue(t, gateway2, key, "before-failure")

	requireAppliedMutation(t, put(
		t, gateway2, key,
		"after-failover", runID, "write-2",
	))

	requireValue(t, gateway1, key, "after-failover")

	failoverDuration := time.Since(failureStarted)

	t.Logf(
		"New leader=%d term=%d",
		newLeader, after.Term,
	)

	requireAppliedMutation(t, put(
		t, gateway1, key,
		"before-failure", runID, "write-1",
	))

	requireValue(t, gateway2, key, "after-failover")

	target, err := fetchGroupStatus(newLeader, shard)
	if err != nil {
		t.Fatal(err)
	}

	recoveryStarted := time.Now()

	startNode(t, leader)

	startupDuration := time.Since(recoveryStarted)

	waitForApplied(
		t, leader, shard, target.CommitIndex,
	)

	recoveryDuration := time.Since(recoveryStarted)

	requireValue(t, gateway1, key, "after-failover")

	measurements := []struct {
		name     string
		duration time.Duration
	}{
		{"docker_stop", stopDuration},
		{"post_stop_election_wait", electionDuration},
		{"end_to_end_failover", failoverDuration},
		{"node_startup", startupDuration},
		{"replica_recovery", recoveryDuration},
	}

	dir := filepath.Join(
		"..", "results", runID,
	)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "fault-metrics.csv")

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)

	if err := writer.Write([]string{
		"run_id",
		"measurement",
		"duration_ms",
		"shard",
		"original_leader",
		"new_leader",
		"original_term",
		"new_term",
		"failure_type",
	}); err != nil {
		t.Fatal(err)
	}

	for _, m := range measurements {
		ms := float64(m.duration) /
			float64(time.Millisecond)

		t.Logf("%s: %.3f ms", m.name, ms)

		if err := writer.Write([]string{
			runID,
			m.name,
			fmt.Sprintf("%.6f", ms),
			fmt.Sprint(shard),
			fmt.Sprint(leader),
			fmt.Sprint(newLeader),
			fmt.Sprint(before.Term),
			fmt.Sprint(after.Term),
			"graceful-compose-stop",
		}); err != nil {
			t.Fatal(err)
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}

	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}

	t.Logf("Fault metrics CSV: %s", path)
	t.Log("Committed value preservation verified")
	t.Log("Duplicate request protection verified")
	t.Log("Replica recovery verified")
}
