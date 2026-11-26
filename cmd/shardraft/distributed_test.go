package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type cliDistributedNode struct {
	id       int
	httpAddr string
	grpcAddr string
	dataDir  string
	args     []string
	process  *exec.Cmd
	done     chan error
}

type cliDistributedCluster struct {
	nodes        []*cliDistributedNode
	daemonBinary string
	cliBinary    string
	httpClient   *http.Client
}

type cliDistributedStatus struct {
	ID           uint64    `json:"id"`
	Role         raft.Role `json:"role"`
	Term         uint64    `json:"term"`
	LeaderID     uint64    `json:"leader_id"`
	CommitIndex  uint64    `json:"commit_index"`
	AppliedIndex uint64    `json:"applied_index"`
}

type cliDistributedResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

func cliDistributedAddresses(
	t *testing.T,
	count int,
) []string {
	t.Helper()

	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)

	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()

	for i := 0; i < count; i++ {
		listener, err := net.Listen(
			"tcp",
			"127.0.0.1:0",
		)
		if err != nil {
			t.Fatal(err)
		}

		listeners = append(listeners, listener)
		addresses = append(
			addresses,
			listener.Addr().String(),
		)
	}

	return addresses
}

func (n *cliDistributedNode) start(
	t *testing.T,
	binary string,
) {
	t.Helper()

	if n.process != nil {
		t.Fatalf("Node %d is already running", n.id)
	}

	logFile, err := os.CreateTemp(
		n.dataDir,
		"process-*.log",
	)
	if err != nil {
		t.Fatal(err)
	}

	process := exec.Command(binary, n.args...)
	process.Stdout = logFile
	process.Stderr = logFile

	if err := process.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}

	done := make(chan error, 1)

	n.process = process
	n.done = done

	go func() {
		err := process.Wait()
		_ = logFile.Close()
		done <- err
	}()

	t.Logf(
		"Started node=%d PID=%d HTTP=%s gRPC=%s",
		n.id,
		process.Process.Pid,
		n.httpAddr,
		n.grpcAddr,
	)
}

func (n *cliDistributedNode) stop(t *testing.T) {
	t.Helper()

	if n.process == nil {
		return
	}

	process := n.process

	defer func() {
		n.process = nil
		n.done = nil
	}()

	err := process.Process.Signal(syscall.SIGTERM)

	if err != nil &&
		!errors.Is(err, os.ErrProcessDone) {
		t.Errorf("Node %d shutdown signal: %v", n.id, err)
	}

	select {
	case err := <-n.done:
		if err != nil {
			t.Errorf(
				"Node %d exited unsuccessfully: %v",
				n.id,
				err,
			)
		}

	case <-time.After(20 * time.Second):
		_ = process.Process.Kill()
		<-n.done

		t.Errorf(
			"Node %d required forced shutdown",
			n.id,
		)
	}

	t.Logf("Stopped node=%d", n.id)
}

func (c *cliDistributedCluster) status(
	node *cliDistributedNode,
) (cliDistributedStatus, error) {
	var result cliDistributedStatus

	response, err := c.httpClient.Get(
		"http://" + node.httpAddr + "/status",
	)
	if err != nil {
		return result, err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf(
			"node %d status returned HTTP %d",
			node.id,
			response.StatusCode,
		)
	}

	if err := json.NewDecoder(
		response.Body,
	).Decode(&result); err != nil {
		return result, err
	}

	if result.ID != uint64(node.id) {
		return result, fmt.Errorf(
			"unexpected node ID: got=%d want=%d",
			result.ID,
			node.id,
		)
	}

	return result, nil
}

func (c *cliDistributedCluster) waitHTTP(
	t *testing.T,
	node *cliDistributedNode,
) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		if _, err := c.status(node); err == nil {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf(
		"Node %d HTTP status not ready",
		node.id,
	)
}

func (c *cliDistributedCluster) waitLeader(
	t *testing.T,
	excludedID int,
) *cliDistributedNode {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)

	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node.id == excludedID ||
				node.process == nil {
				continue
			}

			status, err := c.status(node)

			if err == nil &&
				status.Role == raft.Leader &&
				status.LeaderID == uint64(node.id) &&
				status.CommitIndex > 0 &&
				status.AppliedIndex == status.CommitIndex {
				return node
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("Raft leader was not elected")
	return nil
}

func (c *cliDistributedCluster) waitConvergence(
	t *testing.T,
	minimumCommit uint64,
) cliDistributedStatus {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	stableRounds := 0

	for time.Now().Before(deadline) {
		var reference cliDistributedStatus
		valid := true
		leaders := 0

		for i, node := range c.nodes {
			if node.process == nil {
				valid = false
				break
			}

			status, err := c.status(node)

			if err != nil ||
				status.LeaderID == 0 ||
				status.CommitIndex < minimumCommit ||
				status.AppliedIndex != status.CommitIndex {
				valid = false
				break
			}

			if status.Role == raft.Leader {
				leaders++

				if status.LeaderID != status.ID {
					valid = false
					break
				}
			} else if status.Role != raft.Follower {
				valid = false
				break
			}

			if i == 0 {
				reference = status
				continue
			}

			if status.Term != reference.Term ||
				status.LeaderID != reference.LeaderID ||
				status.CommitIndex != reference.CommitIndex ||
				status.AppliedIndex != reference.AppliedIndex {
				valid = false
				break
			}
		}

		if valid && leaders == 1 {
			stableRounds++

			if stableRounds >= 5 {
				t.Logf(
					"Cluster converged: leader=%d term=%d commit=%d applied=%d",
					reference.LeaderID,
					reference.Term,
					reference.CommitIndex,
					reference.AppliedIndex,
				)

				return reference
			}
		} else {
			stableRounds = 0
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("Cluster did not converge")
	return cliDistributedStatus{}
}

func (c *cliDistributedCluster) endpoints() string {
	addresses := make([]string, 0, len(c.nodes))

	for _, node := range c.nodes {
		addresses = append(addresses, node.grpcAddr)
	}

	return strings.Join(addresses, ",")
}

func (c *cliDistributedCluster) follower(
	t *testing.T,
	leader *cliDistributedNode,
) *cliDistributedNode {
	t.Helper()

	for _, node := range c.nodes {
		if node != leader && node.process != nil {
			return node
		}
	}

	t.Fatal("No running follower")
	return nil
}

func (c *cliDistributedCluster) execute(
	t *testing.T,
	args ...string,
) cliDistributedResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	command := exec.CommandContext(
		ctx,
		c.cliBinary,
		args...,
	)

	command.Stdout = &stdout
	command.Stderr = &stderr

	err := command.Run()

	result := cliDistributedResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}

	if ctx.Err() != nil {
		t.Fatalf(
			"CLI process timed out: args=%q stderr=%s",
			args,
			result.Stderr,
		)
	}

	if err != nil {
		var exitError *exec.ExitError

		if !errors.As(err, &exitError) {
			t.Fatalf("CLI execution failed: %v", err)
		}

		result.ExitCode = exitError.ExitCode()

		if result.ExitCode < 0 {
			t.Fatalf("CLI terminated unexpectedly: %v", err)
		}
	}

	return result
}

func cliDistributedRequireExit(
	t *testing.T,
	result cliDistributedResult,
	expected int,
) {
	t.Helper()

	if result.ExitCode != expected {
		t.Fatalf(
			"CLI exit=%d want=%d stdout=%q stderr=%q",
			result.ExitCode,
			expected,
			result.Stdout,
			result.Stderr,
		)
	}
}

func newCLIDistributedCluster(
	t *testing.T,
) *cliDistributedCluster {
	t.Helper()

	if os.Getenv("SHARDRAFT_CLI_INTEGRATION") != "1" {
		t.Skip("Set SHARDRAFT_CLI_INTEGRATION=1")
	}

	daemonBinary := os.Getenv("SHARDRAFT_DAEMON_BINARY")
	cliBinary := os.Getenv("SHARDRAFT_CLI_BINARY")

	if daemonBinary == "" || cliBinary == "" {
		t.Fatal("Integration binary paths are required")
	}

	var err error

	daemonBinary, err = filepath.Abs(daemonBinary)
	if err != nil {
		t.Fatal(err)
	}

	cliBinary, err = filepath.Abs(cliBinary)
	if err != nil {
		t.Fatal(err)
	}

	for _, binary := range []string{
		daemonBinary,
		cliBinary,
	} {
		info, err := os.Stat(binary)

		if err != nil {
			t.Fatal(err)
		}

		if !info.Mode().IsRegular() ||
			info.Mode().Perm()&0111 == 0 {
			t.Fatalf("Invalid executable: %s", binary)
		}
	}

	addresses := cliDistributedAddresses(t, 6)
	root := t.TempDir()

	cluster := &cliDistributedCluster{
		daemonBinary: daemonBinary,
		cliBinary:    cliBinary,
		httpClient: &http.Client{
			Timeout: 500 * time.Millisecond,
		},
	}

	t.Cleanup(func() {
		cluster.httpClient.CloseIdleConnections()

		for _, node := range cluster.nodes {
			node.stop(t)
		}

		if t.Failed() {
			files, _ := filepath.Glob(
				filepath.Join(root, "*", "*.log"),
			)

			for _, file := range files {
				data, err := os.ReadFile(file)
				if err == nil {
					t.Logf("%s:\n%s", file, data)
				}
			}
		}
	})

	for i := 0; i < 3; i++ {
		id := i + 1

		node := &cliDistributedNode{
			id:       id,
			httpAddr: addresses[i],
			grpcAddr: addresses[i+3],
			dataDir: filepath.Join(
				root,
				fmt.Sprintf("node-%d", id),
			),
		}

		if err := os.MkdirAll(
			node.dataDir,
			0700,
		); err != nil {
			t.Fatal(err)
		}

		var peers []string
		var httpPeers []string
		var grpcPeers []string

		for j := 0; j < 3; j++ {
			if i == j {
				continue
			}

			peers = append(
				peers,
				fmt.Sprint(j+1),
			)

			httpPeers = append(
				httpPeers,
				fmt.Sprintf(
					"%d=%s",
					j+1,
					addresses[j],
				),
			)

			grpcPeers = append(
				grpcPeers,
				fmt.Sprintf(
					"%d=%s",
					j+1,
					addresses[j+3],
				),
			)
		}

		node.args = []string{
			"-id", fmt.Sprint(id),
			"-peers", strings.Join(peers, ","),
			"-peer-addrs", strings.Join(httpPeers, ","),
			"-data-dir", node.dataDir,
			"-election-timeout", fmt.Sprint(15 + i*10),
			"-tick-interval", "20ms",
			"-http-addr", node.httpAddr,
			"-raft-transport", "grpc",
			"-grpc-addr", node.grpcAddr,
			"-grpc-peer-addrs", strings.Join(grpcPeers, ","),
		}

		cluster.nodes = append(cluster.nodes, node)
		node.start(t, daemonBinary)
	}

	for _, node := range cluster.nodes {
		cluster.waitHTTP(t, node)
	}

	leader := cluster.waitLeader(t, 0)
	cluster.waitConvergence(t, 1)

	t.Logf(
		"CLI cluster ready: leader=%d endpoints=%s",
		leader.id,
		cluster.endpoints(),
	)

	return cluster
}

func TestCLIDistributedHarness(t *testing.T) {
	cluster := newCLIDistributedCluster(t)

	if len(cluster.nodes) != 3 {
		t.Fatal("Expected three daemon processes")
	}

	leader := cluster.waitLeader(t, 0)
	follower := cluster.follower(t, leader)

	if leader.id == follower.id {
		t.Fatal("Leader and follower must be distinct")
	}

	ports := make(map[string]bool)
	directories := make(map[string]bool)
	pids := make(map[int]bool)

	for _, node := range cluster.nodes {
		for _, address := range []string{
			node.httpAddr,
			node.grpcAddr,
		} {
			if ports[address] {
				t.Fatalf("Duplicate address: %s", address)
			}

			ports[address] = true
		}

		if directories[node.dataDir] {
			t.Fatal("Shared node data directory")
		}

		directories[node.dataDir] = true

		if node.process == nil {
			t.Fatal("Missing daemon process")
		}

		pid := node.process.Process.Pid

		if pids[pid] {
			t.Fatal("Duplicate daemon PID")
		}

		pids[pid] = true
	}

	help := cluster.execute(t, "--help")

	cliDistributedRequireExit(
		t,
		help,
		cliExitSuccess,
	)

	if help.Stdout == "" || help.Stderr != "" {
		t.Fatalf("Unexpected help output: %+v", help)
	}

	missing := cluster.execute(
		t,
		"--grpc-endpoints", follower.grpcAddr,
		"--timeout", "11s",
		"--output", "json",
		"get", "harness-missing-key",
	)

	cliDistributedRequireExit(
		t,
		missing,
		cliExitNotFound,
	)

	if missing.Stderr != "" {
		t.Fatalf(
			"Unexpected GET stderr: %s",
			missing.Stderr,
		)
	}

	var result cliGetResult

	if err := json.Unmarshal(
		[]byte(missing.Stdout),
		&result,
	); err != nil {
		t.Fatal(err)
	}

	if result.Key != "harness-missing-key" ||
		result.Found ||
		result.Value != "" ||
		result.ReadMode != "linearizable" {
		t.Fatalf("Unexpected GET response: %+v", result)
	}

	cluster.waitConvergence(t, 1)

	cluster.httpClient.CloseIdleConnections()

	for _, node := range cluster.nodes {
		node.stop(t)
	}

	for address := range ports {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf(
				"Listener was not released: %s: %v",
				address,
				err,
			)
		}

		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}

	t.Log("Verified three distinct daemon processes")
	t.Log("Verified six independent listening addresses")
	t.Log("Verified three isolated data directories")
	t.Log("Verified compiled CLI execution and exit codes")
	t.Log("Verified follower-bootstrap linearizable GET")
	t.Log("Verified graceful shutdown and listener release")
}
