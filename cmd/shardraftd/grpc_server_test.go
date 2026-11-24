package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type grpcLifecycleTestNode struct{}

func (n *grpcLifecycleTestNode) Status() (raft.Status, error) {
	return raft.Status{
		ID:   1,
		Role: raft.Follower,
		Term: 1,
	}, nil
}

func (n *grpcLifecycleTestNode) Step(message raft.Message) error {
	return nil
}

func lifecycleFreeAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address := listener.Addr().String()

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	return address
}

func lifecycleGRPCClient(
	t *testing.T,
	address string,
) shardraftpb.RaftServiceClient {
	t.Helper()

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
	})

	return shardraftpb.NewRaftServiceClient(conn)
}

func lifecycleWait(
	t *testing.T,
	condition func() bool,
) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("daemon readiness condition timed out")
}

func lifecycleAssertPortReleased(
	t *testing.T,
	address string,
) {
	t.Helper()

	lifecycleWait(t, func() bool {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return false
		}

		_ = listener.Close()
		return true
	})
}

func lifecycleStartDaemon(
	t *testing.T,
	cfg config,
) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	done := make(chan error, 1)

	go func() {
		done <- run(ctx, cfg)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()

		select {
		case <-done:
		case <-time.After(12 * time.Second):
			t.Error("daemon cleanup timed out")
		}
	})

	return cancel, done
}

func lifecycleStopDaemon(
	t *testing.T,
	cancel context.CancelFunc,
	done <-chan error,
) {
	t.Helper()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("daemon shutdown timed out")
	}
}

func lifecycleWaitHTTP(
	t *testing.T,
	address string,
) {
	t.Helper()

	client := &http.Client{
		Timeout: 250 * time.Millisecond,
	}

	lifecycleWait(t, func() bool {
		response, err := client.Get(
			"http://" + address + "/status",
		)
		if err != nil {
			return false
		}

		defer response.Body.Close()

		return response.StatusCode == http.StatusOK
	})
}

func lifecycleWaitGRPC(
	t *testing.T,
	address string,
) {
	t.Helper()

	client := lifecycleGRPCClient(t, address)

	lifecycleWait(t, func() bool {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			250*time.Millisecond,
		)
		defer cancel()

		_, err := client.Step(
			ctx,
			&shardraftpb.StepRequest{},
		)

		// A validation error proves the Raft service
		// is registered and processing requests.
		return status.Code(err) == codes.InvalidArgument
	})
}

func lifecycleConfig(t *testing.T) config {
	t.Helper()

	return config{
		id:              1,
		dataDir:         t.TempDir(),
		electionTimeout: 5,
		tickInterval:    10 * time.Millisecond,
		raftTransport:   "grpc",
		grpcAddr:        lifecycleFreeAddress(t),
	}
}

func TestDaemonGRPCServerStartup(t *testing.T) {
	server, err := startDaemonGRPCServer(
		"127.0.0.1:0",
		&grpcLifecycleTestNode{},
		[]raft.NodeID{2},
	)
	if err != nil {
		t.Fatal(err)
	}

	defer server.Close()

	if server.Addr() == nil {
		t.Fatal("missing gRPC listener address")
	}

	lifecycleWaitGRPC(t, server.Addr().String())
}

func TestDaemonGRPCServiceRegistration(t *testing.T) {
	server, err := startDaemonGRPCServer(
		"127.0.0.1:0",
		&grpcLifecycleTestNode{},
		[]raft.NodeID{2},
	)
	if err != nil {
		t.Fatal(err)
	}

	defer server.Close()

	client := lifecycleGRPCClient(
		t,
		server.Addr().String(),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	response, err := client.Step(
		ctx,
		&shardraftpb.StepRequest{
			ShardId: 1,
			Message: &shardraftpb.RaftMessage{
				Type: shardraftpb.MessageType_MSG_HEARTBEAT,
				From: 2,
				To:   1,
				Term: 1,
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if !response.Accepted {
		t.Fatal("Raft message was not accepted")
	}
}

func TestDaemonGRPCOccupiedPort(t *testing.T) {
	listener, err := net.Listen(
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}

	defer listener.Close()

	server, err := startDaemonGRPCServer(
		listener.Addr().String(),
		&grpcLifecycleTestNode{},
		nil,
	)

	if err == nil {
		server.Close()
		t.Fatal("expected occupied port error")
	}
}

func TestDaemonGRPCInvalidTransport(t *testing.T) {
	cfg := lifecycleConfig(t)
	cfg.raftTransport = "invalid"

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := run(ctx, cfg)

	if err == nil ||
		!strings.Contains(err.Error(), "transport") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDaemonGRPCHTTPCoexistence(t *testing.T) {
	cfg := lifecycleConfig(t)
	cfg.httpAddr = lifecycleFreeAddress(t)

	for cfg.httpAddr == cfg.grpcAddr {
		cfg.httpAddr = lifecycleFreeAddress(t)
	}

	cancel, done := lifecycleStartDaemon(t, cfg)

	lifecycleWaitHTTP(t, cfg.httpAddr)
	lifecycleWaitGRPC(t, cfg.grpcAddr)

	lifecycleStopDaemon(t, cancel, done)

	lifecycleAssertPortReleased(t, cfg.httpAddr)
	lifecycleAssertPortReleased(t, cfg.grpcAddr)
}

func TestDaemonGRPCGracefulShutdown(t *testing.T) {
	cfg := lifecycleConfig(t)

	cancel, done := lifecycleStartDaemon(t, cfg)

	lifecycleWaitGRPC(t, cfg.grpcAddr)

	lifecycleStopDaemon(t, cancel, done)

	lifecycleAssertPortReleased(t, cfg.grpcAddr)
}

func TestDaemonGRPCRestart(t *testing.T) {
	cfg := lifecycleConfig(t)

	for attempt := 0; attempt < 2; attempt++ {
		cancel, done := lifecycleStartDaemon(t, cfg)

		lifecycleWaitGRPC(t, cfg.grpcAddr)

		lifecycleStopDaemon(t, cancel, done)

		lifecycleAssertPortReleased(t, cfg.grpcAddr)
	}
}

func TestDaemonGRPCStartupFailureCleanup(t *testing.T) {
	listener, err := net.Listen(
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}

	defer listener.Close()

	cfg := lifecycleConfig(t)
	cfg.httpAddr = listener.Addr().String()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	err = run(ctx, cfg)

	if err == nil ||
		!strings.Contains(err.Error(), "HTTP listener") {
		t.Fatalf("unexpected startup error: %v", err)
	}

	lifecycleAssertPortReleased(t, cfg.grpcAddr)
}

func TestDaemonGRPCHTTPDefaultCompatibility(t *testing.T) {
	cfg := lifecycleConfig(t)

	cfg.raftTransport = ""
	cfg.grpcAddr = ""
	cfg.httpAddr = lifecycleFreeAddress(t)

	cancel, done := lifecycleStartDaemon(t, cfg)

	lifecycleWaitHTTP(t, cfg.httpAddr)

	lifecycleStopDaemon(t, cancel, done)

	lifecycleAssertPortReleased(t, cfg.httpAddr)
}

func TestDaemonGRPCServerErrorShutdown(t *testing.T) {
	server, err := startDaemonGRPCServer(
		"127.0.0.1:0",
		&grpcLifecycleTestNode{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	address := server.Addr().String()

	lifecycleWaitGRPC(t, address)

	if err := server.listener.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-server.Errors():
		if err == nil {
			t.Fatal("expected listener failure")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server failure was not reported")
	}

	done := make(chan struct{})

	go func() {
		server.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(7 * time.Second):
		t.Fatal("server shutdown timed out")
	}

	lifecycleAssertPortReleased(t, address)
}
