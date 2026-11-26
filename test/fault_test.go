package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"testing"
	"time"
)

type groupStatus struct {
	ShardID      uint64 `json:"shard_id"`
	Role         string `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     uint64 `json:"leader_id"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
}

type nodeStatus struct {
	ID     uint64        `json:"id"`
	Mode   string        `json:"mode"`
	Groups []groupStatus `json:"groups"`
}

func fetchNodeStatus(
	node uint64,
) (nodeStatus, error) {
	var result nodeStatus

	client := &http.Client{
		Timeout: time.Second,
	}

	url := fmt.Sprintf(
		"http://127.0.0.1:%d/status",
		18080+node,
	)

	resp, err := client.Get(url)

	if err != nil {
		return result, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf(
			"node %d returned status %d",
			node,
			resp.StatusCode,
		)
	}

	err = json.NewDecoder(resp.Body).Decode(&result)

	if err != nil {
		return result, err
	}

	if result.ID != node {
		return result, fmt.Errorf(
			"unexpected node identity: %d",
			result.ID,
		)
	}

	return result, nil
}

func fetchGroupStatus(
	node uint64,
	shard uint64,
) (groupStatus, error) {
	status, err := fetchNodeStatus(node)

	if err != nil {
		return groupStatus{}, err
	}

	for _, group := range status.Groups {
		if group.ShardID == shard {
			return group, nil
		}
	}

	return groupStatus{}, fmt.Errorf(
		"node %d does not host shard %d",
		node,
		shard,
	)
}

func waitForLeader(
	t *testing.T,
	shard uint64,
	replicas []uint64,
	excluded uint64,
	minimumTerm uint64,
) (uint64, groupStatus) {
	t.Helper()

	deadline := time.Now().Add(25 * time.Second)

	for time.Now().Before(deadline) {
		var leader uint64
		var state groupStatus
		count := 0

		for _, node := range replicas {
			if node == excluded {
				continue
			}

			group, err := fetchGroupStatus(
				node,
				shard,
			)

			if err != nil {
				continue
			}

			if group.Role == "leader" &&
				group.LeaderID == node &&
				group.Term >= minimumTerm {
				leader = node
				state = group
				count++
			}
		}

		if count == 1 {
			return leader, state
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf(
		"no unique leader for shard %d",
		shard,
	)

	return 0, groupStatus{}
}

func composeNode(
	action string,
	node uint64,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)

	defer cancel()

	cmd := exec.CommandContext(
		ctx,
		"docker",
		"compose",
		"-f",
		"../docker-compose.yml",
		"-p",
		"shardraft",
		action,
		fmt.Sprintf("n%d", node),
	)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf(
			"%s node %d: %w: %s",
			action,
			node,
			err,
			output,
		)
	}

	return nil
}

func stopNode(
	t *testing.T,
	node uint64,
) {
	t.Helper()

	t.Cleanup(func() {
		if err := composeNode("start", node); err != nil {
			t.Errorf(
				"restore node %d: %v",
				node,
				err,
			)
		}
	})

	if err := composeNode("stop", node); err != nil {
		t.Fatal(err)
	}

	if _, err := fetchNodeStatus(node); err == nil {
		t.Fatalf("node %d remains reachable", node)
	}

	t.Logf("node %d stopped", node)
}

func startNode(
	t *testing.T,
	node uint64,
) {
	t.Helper()

	if err := composeNode("start", node); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(25 * time.Second)

	for time.Now().Before(deadline) {
		if _, err := fetchNodeStatus(node); err == nil {
			t.Logf("node %d restarted", node)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("node %d did not recover", node)
}

func waitForApplied(
	t *testing.T,
	node uint64,
	shard uint64,
	target uint64,
) {
	t.Helper()

	deadline := time.Now().Add(25 * time.Second)

	for time.Now().Before(deadline) {
		state, err := fetchGroupStatus(node, shard)

		if err == nil &&
			state.CommitIndex >= target &&
			state.AppliedIndex >= target {
			t.Logf(
				"node=%d shard=%d commit=%d applied=%d target=%d",
				node,
				shard,
				state.CommitIndex,
				state.AppliedIndex,
				target,
			)

			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf(
		"node %d did not catch up to index %d",
		node,
		target,
	)
}

func requireAppliedMutation(
	t *testing.T,
	result kvResponse,
) {
	t.Helper()

	if !result.Applied {
		t.Fatal("mutation was not applied")
	}
}

func TestDockerLeaderFailover(t *testing.T) {
	requireDocker(t)

	const shard = uint64(1)

	replicas := []uint64{1, 2, 3}

	runID := fmt.Sprintf(
		"leader-failover-%d",
		time.Now().UnixNano(),
	)

	key := keyForShard(t, runID, uint32(shard))

	leader, before := waitForLeader(
		t,
		shard,
		replicas,
		0,
		1,
	)

	t.Logf(
		"original leader=%d term=%d key=%s",
		leader,
		before.Term,
		key,
	)

	requireAppliedMutation(
		t,
		put(
			t,
			gateway1,
			key,
			"committed-before-failure",
			runID,
			"original-write",
		),
	)

	requireValue(
		t,
		gateway2,
		key,
		"committed-before-failure",
	)

	stopNode(t, leader)

	started := time.Now()

	newLeader, after := waitForLeader(
		t,
		shard,
		replicas,
		leader,
		before.Term+1,
	)

	t.Logf(
		"new leader=%d term=%d observed election wait=%s",
		newLeader,
		after.Term,
		time.Since(started),
	)

	requireValue(
		t,
		gateway2,
		key,
		"committed-before-failure",
	)

	requireAppliedMutation(
		t,
		put(
			t,
			gateway2,
			key,
			"committed-after-failover",
			runID,
			"new-write",
		),
	)

	requireAppliedMutation(
		t,
		put(
			t,
			gateway1,
			key,
			"committed-before-failure",
			runID,
			"original-write",
		),
	)

	requireValue(
		t,
		gateway2,
		key,
		"committed-after-failover",
	)

	t.Log("original request retry preserved newer committed value")

	target, err := fetchGroupStatus(
		newLeader,
		shard,
	)

	if err != nil {
		t.Fatal(err)
	}

	startNode(t, leader)

	waitForApplied(
		t,
		leader,
		shard,
		target.CommitIndex,
	)

	requireValue(
		t,
		gateway1,
		key,
		"committed-after-failover",
	)

	t.Log("leader restart and replica catch-up passed")
}

func TestDockerFollowerLoss(t *testing.T) {
	requireDocker(t)

	const shard = uint64(1)

	replicas := []uint64{1, 2, 3}

	runID := fmt.Sprintf(
		"follower-loss-%d",
		time.Now().UnixNano(),
	)

	key := keyForShard(t, runID, uint32(shard))

	leader, before := waitForLeader(
		t,
		shard,
		replicas,
		0,
		1,
	)

	var follower uint64

	for _, node := range replicas {
		if node == leader {
			continue
		}

		state, err := fetchGroupStatus(node, shard)

		if err == nil &&
			state.Role == "follower" &&
			state.LeaderID == leader {
			follower = node
			break
		}
	}

	if follower == 0 {
		t.Fatal("no available follower")
	}

	t.Logf(
		"leader=%d follower=%d term=%d",
		leader,
		follower,
		before.Term,
	)

	requireAppliedMutation(
		t,
		put(
			t,
			gateway1,
			key,
			"before-follower-loss",
			runID,
			"write-1",
		),
	)

	initial, err := fetchGroupStatus(leader, shard)

	if err != nil {
		t.Fatal(err)
	}

	waitForApplied(
		t,
		follower,
		shard,
		initial.CommitIndex,
	)

	stopNode(t, follower)

	requireAppliedMutation(
		t,
		put(
			t,
			gateway2,
			key,
			"while-follower-offline",
			runID,
			"write-2",
		),
	)

	requireValue(
		t,
		gateway1,
		key,
		"while-follower-offline",
	)

	currentLeader, current := waitForLeader(
		t,
		shard,
		replicas,
		follower,
		before.Term,
	)

	t.Logf(
		"quorum remained available leader=%d term=%d",
		currentLeader,
		current.Term,
	)

	startNode(t, follower)

	waitForApplied(
		t,
		follower,
		shard,
		current.CommitIndex,
	)

	requireValue(
		t,
		gateway2,
		key,
		"while-follower-offline",
	)

	requireAppliedMutation(
		t,
		put(
			t,
			gateway1,
			key,
			"after-follower-recovery",
			runID,
			"write-3",
		),
	)

	requireValue(
		t,
		gateway2,
		key,
		"after-follower-recovery",
	)

	t.Log("follower loss, restart and recovery passed")
}
