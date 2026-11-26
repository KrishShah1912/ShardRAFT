package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

type gatewayDistributedNode struct {
	id       int
	httpAddr string
	grpcAddr string
	dataDir  string
	args     []string
	process  *exec.Cmd
	done     chan error
}

type gatewayDistributedCluster struct {
	nodes         []*gatewayDistributedNode
	gatewayURL    string
	gatewayBinary string
	daemonBinary  string
	gateway       *gatewayDistributedNode
	httpClient    *http.Client
}

type gatewayDistributedResponse struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Found     bool   `json:"found"`
	Applied   bool   `json:"applied"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
	Error     string `json:"error"`
	Code      string `json:"code"`
}

type gatewayDistributedStatus struct {
	ID           uint64    `json:"id"`
	Role         raft.Role `json:"role"`
	Term         uint64    `json:"term"`
	LeaderID     uint64    `json:"leader_id"`
	CommitIndex  uint64    `json:"commit_index"`
	AppliedIndex uint64    `json:"applied_index"`
}

func gatewayDistributedAddresses(
	t *testing.T,
	count int,
) []string {
	t.Helper()

	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)

	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()

	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}

	return addresses
}

func (n *gatewayDistributedNode) start(
	t *testing.T,
	binary string,
) {
	t.Helper()

	logFile, err := os.CreateTemp(
		n.dataDir,
		"process-*.log",
	)
	if err != nil {
		t.Fatal(err)
	}

	n.process = exec.Command(binary, n.args...)
	n.process.Stdout = logFile
	n.process.Stderr = logFile

	if err := n.process.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}

	n.done = make(chan error, 1)

	process := n.process
	done := n.done

	go func() {
		err := process.Wait()
		logFile.Close()
		done <- err
	}()

	t.Logf(
		"Started process PID=%d address=%s",
		n.process.Process.Pid,
		n.httpAddr,
	)
}

func (n *gatewayDistributedNode) stop(t *testing.T) {
	t.Helper()

	if n.process == nil {
		return
	}

	process := n.process

	defer func() {
		n.process = nil
	}()

	if err := process.Process.Signal(syscall.SIGTERM); err != nil {
		if err != os.ErrProcessDone {
			t.Logf("Process signal: %v", err)
		}
	}

	select {
	case err := <-n.done:
		if err != nil {
			t.Errorf("Process exited unsuccessfully: %v", err)
		}

	case <-time.After(20 * time.Second):
		_ = process.Process.Kill()
		<-n.done
		t.Error("Process required forced shutdown")
	}
}

func gatewayDistributedWaitHTTP(
	t *testing.T,
	address string,
	path string,
	expectedStatus int,
) {
	t.Helper()

	c := &http.Client{
		Timeout: 500 * time.Millisecond,
	}
	defer c.CloseIdleConnections()

	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		response, err := c.Get(
			"http://" + address + path,
		)

		if err == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()

			if response.StatusCode == expectedStatus {
				return
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("HTTP listener not ready: %s", address)
}

func newGatewayDistributedCluster(
	t *testing.T,
) *gatewayDistributedCluster {
	t.Helper()

	if os.Getenv("SHARDRAFT_GATEWAY_INTEGRATION") != "1" {
		t.Skip("Set SHARDRAFT_GATEWAY_INTEGRATION=1")
	}

	daemonBinary := os.Getenv("SHARDRAFT_DAEMON_BINARY")
	gatewayBinary := os.Getenv("SHARDRAFT_GATEWAY_BINARY")

	if daemonBinary == "" || gatewayBinary == "" {
		t.Fatal("Integration binary paths are required")
	}

	addresses := gatewayDistributedAddresses(t, 7)
	root := t.TempDir()

	cluster := &gatewayDistributedCluster{
		daemonBinary:  daemonBinary,
		gatewayBinary: gatewayBinary,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}

	t.Cleanup(func() {
		cluster.httpClient.CloseIdleConnections()

		if cluster.gateway != nil {
			cluster.gateway.stop(t)
		}

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

		node := &gatewayDistributedNode{
			id:       id,
			httpAddr: addresses[i],
			grpcAddr: addresses[i+3],
			dataDir: filepath.Join(
				root,
				fmt.Sprintf("node-%d", id),
			),
		}

		if err := os.MkdirAll(node.dataDir, 0700); err != nil {
			t.Fatal(err)
		}

		var peers []string
		var grpcPeers []string
		var httpPeers []string

		for j := 0; j < 3; j++ {
			if i == j {
				continue
			}

			peers = append(peers, fmt.Sprint(j+1))

			grpcPeers = append(
				grpcPeers,
				fmt.Sprintf("%d=%s", j+1, addresses[j+3]),
			)

			httpPeers = append(
				httpPeers,
				fmt.Sprintf("%d=%s", j+1, addresses[j]),
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
		gatewayDistributedWaitHTTP(
			t,
			node.httpAddr,
			"/status",
			http.StatusOK,
		)
	}

	leader := cluster.waitLeader(t, 0)

	var follower *gatewayDistributedNode

	for _, node := range cluster.nodes {
		if node != leader {
			follower = node
			break
		}
	}

	gatewayDir := filepath.Join(root, "gateway")

	if err := os.MkdirAll(gatewayDir, 0700); err != nil {
		t.Fatal(err)
	}

	cluster.gateway = &gatewayDistributedNode{
		httpAddr: addresses[6],
		dataDir:  gatewayDir,
		args: []string{
			"-http-addr", addresses[6],
			"-grpc-endpoints", follower.grpcAddr,
			"-request-timeout", "11s",
		},
	}

	cluster.gatewayURL = "http://" + addresses[6]

	cluster.gateway.start(t, gatewayBinary)

	gatewayDistributedWaitHTTP(
		t,
		addresses[6],
		"/nonexistent",
		http.StatusNotFound,
	)

	t.Logf(
		"Cluster ready: leader=%d bootstrap follower=%d",
		leader.id,
		follower.id,
	)

	return cluster
}

func (c *gatewayDistributedCluster) status(
	node *gatewayDistributedNode,
) (gatewayDistributedStatus, error) {
	var status gatewayDistributedStatus

	client := &http.Client{
		Timeout: 500 * time.Millisecond,
	}
	defer client.CloseIdleConnections()

	response, err := client.Get(
		"http://" + node.httpAddr + "/status",
	)
	if err != nil {
		return status, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return status, fmt.Errorf(
			"status endpoint returned %d",
			response.StatusCode,
		)
	}

	err = json.NewDecoder(response.Body).Decode(&status)

	return status, err
}

func (c *gatewayDistributedCluster) waitLeader(
	t *testing.T,
	excludedID int,
) *gatewayDistributedNode {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)

	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node.id == excludedID || node.process == nil {
				continue
			}

			status, err := c.status(node)
			if err != nil {
				t.Logf("Node %d status error: %v", node.id, err)
				continue
			}

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

func (c *gatewayDistributedCluster) request(
	t *testing.T,
	method string,
	path string,
	body any,
	expectedStatus int,
) gatewayDistributedResponse {
	t.Helper()

	var result gatewayDistributedResponse
	var payload []byte

	if body != nil {
		var err error
		payload, err = json.Marshal(body)

		if err != nil {
			t.Fatal(err)
		}
	}

	request, err := http.NewRequest(
		method,
		c.gatewayURL+path,
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatal(err)
	}

	if body != nil {
		request.Header.Set(
			"Content-Type",
			"application/json",
		)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}

	if response.StatusCode != expectedStatus {
		t.Fatalf(
			"%s %s: status=%d want=%d body=%s",
			method,
			path,
			response.StatusCode,
			expectedStatus,
			data,
		)
	}

	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Invalid JSON response: %s: %v", data, err)
	}

	return result
}

func gatewayDistributedMutation(
	requestID string,
	fields map[string]string,
) map[string]string {
	result := map[string]string{
		"client_id":  "gateway-integration-client",
		"request_id": requestID,
	}

	for key, value := range fields {
		result[key] = value
	}

	return result
}

func gatewayDistributedRequireMutation(
	t *testing.T,
	response gatewayDistributedResponse,
	requestID string,
	applied bool,
) {
	t.Helper()

	if response.Applied != applied ||
		response.ClientID != "gateway-integration-client" ||
		response.RequestID != requestID ||
		response.Error != "" {
		t.Fatalf("Unexpected mutation response: %+v", response)
	}
}

func TestGatewayDistributedBootstrapDiscovery(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	result := cluster.request(
		t,
		http.MethodPut,
		"/kv/discovery-key",
		gatewayDistributedMutation(
			"discovery-put",
			map[string]string{"value": "100"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "discovery-put", true,
	)

	result = cluster.request(
		t,
		http.MethodGet,
		"/kv/discovery-key",
		nil,
		http.StatusOK,
	)

	if !result.Found || result.Value != "100" {
		t.Fatalf("Unexpected read response: %+v", result)
	}

	t.Log("Follower bootstrap and automatic leader routing passed")
}

func TestGatewayDistributedCRUD(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const path = "/kv/production-key"

	result := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			"crud-put",
			map[string]string{"value": "100"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "crud-put", true,
	)

	result = cluster.request(
		t, http.MethodGet, path, nil, http.StatusOK,
	)

	if !result.Found ||
		result.Key != "production-key" ||
		result.Value != "100" {
		t.Fatalf("Unexpected GET response: %+v", result)
	}

	result = cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			"crud-update",
			map[string]string{"value": "200"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "crud-update", true,
	)

	result = cluster.request(
		t, http.MethodGet, path, nil, http.StatusOK,
	)

	if !result.Found || result.Value != "200" {
		t.Fatalf("Unexpected updated value: %+v", result)
	}

	result = cluster.request(
		t,
		http.MethodDelete,
		path,
		gatewayDistributedMutation("crud-delete", nil),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "crud-delete", true,
	)

	result = cluster.request(
		t, http.MethodGet, path, nil, http.StatusNotFound,
	)

	if result.Code != "key_not_found" {
		t.Fatalf("Unexpected missing-key response: %+v", result)
	}

	t.Log("Distributed HTTP-to-gRPC CRUD passed")
}

func TestGatewayDistributedReadModes(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	result := cluster.request(
		t,
		http.MethodPut,
		"/kv/read-key",
		gatewayDistributedMutation(
			"read-put",
			map[string]string{"value": "100"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "read-put", true,
	)

	for _, mode := range []string{"", "linearizable", "local"} {
		path := "/kv/read-key"

		if mode != "" {
			path += "?read_mode=" + mode
		}

		result = cluster.request(
			t, http.MethodGet, path, nil, http.StatusOK,
		)

		if !result.Found || result.Value != "100" {
			t.Fatalf("Read mode %q: %+v", mode, result)
		}
	}

	t.Log("Default, linearizable and local reads passed")
}

func TestGatewayDistributedCAS(t *testing.T) {
	cluster := newGatewayDistributedCluster(t)

	const path = "/kv/cas-key"

	result := cluster.request(
		t,
		http.MethodPut,
		path,
		gatewayDistributedMutation(
			"cas-initial",
			map[string]string{"value": "100"},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "cas-initial", true,
	)

	result = cluster.request(
		t,
		http.MethodPost,
		path+"/cas",
		gatewayDistributedMutation(
			"cas-success",
			map[string]string{
				"expected": "100",
				"value":    "200",
			},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "cas-success", true,
	)

	result = cluster.request(
		t,
		http.MethodPost,
		path+"/cas",
		gatewayDistributedMutation(
			"cas-mismatch",
			map[string]string{
				"expected": "100",
				"value":    "300",
			},
		),
		http.StatusOK,
	)

	gatewayDistributedRequireMutation(
		t, result, "cas-mismatch", false,
	)

	result = cluster.request(
		t, http.MethodGet, path, nil, http.StatusOK,
	)

	if !result.Found || result.Value != "200" {
		t.Fatalf("CAS mismatch changed value: %+v", result)
	}

	t.Log("Distributed CAS success and mismatch passed")
}
