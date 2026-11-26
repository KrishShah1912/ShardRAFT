package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/meta"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func multiDaemonTestConfig(t *testing.T) config {
	t.Helper()

	return config{
		multiRaft:       true,
		id:              3,
		peers:           "1,2,4,5",
		dataDir:         t.TempDir(),
		electionTimeout: 100,
		tickInterval:    20 * time.Millisecond,
		httpAddr:        "127.0.0.1:0",
		raftTransport:   raftTransportGRPC,
		grpcAddr:        "127.0.0.1:0",
		grpcPeerAddrs: "1=127.0.0.1:19091," +
			"2=127.0.0.1:19092," +
			"4=127.0.0.1:19094," +
			"5=127.0.0.1:19095",
	}
}

func TestMultiDaemonConfiguration(t *testing.T) {
	base := multiDaemonTestConfig(t)

	if err := validateMultiRaftConfig(base); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		change func(*config)
	}{
		{
			"invalid-node",
			func(c *config) { c.id = 6 },
		},
		{
			"empty-directory",
			func(c *config) { c.dataDir = "" },
		},
		{
			"invalid-election-timeout",
			func(c *config) { c.electionTimeout = 0 },
		},
		{
			"invalid-tick",
			func(c *config) { c.tickInterval = 0 },
		},
		{
			"http-transport",
			func(c *config) {
				c.raftTransport = raftTransportHTTP
			},
		},
		{
			"missing-grpc-address",
			func(c *config) { c.grpcAddr = "" },
		},
		{
			"missing-peers",
			func(c *config) { c.peers = "1,2" },
		},
		{
			"self-peer",
			func(c *config) { c.peers = "1,2,3,5" },
		},
		{
			"missing-peer-addresses",
			func(c *config) { c.grpcPeerAddrs = "" },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.change(&cfg)

			if err := validateMultiRaftConfig(cfg); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestMultiDaemonStatus(t *testing.T) {
	manager := newMultiRaftTestManager(t)
	handler := multiRaftStatusHandler(manager, 3)

	response := httptest.NewRecorder()

	handler.ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodGet,
			"/status",
			nil,
		),
	)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"status=%d body=%s",
			response.Code,
			response.Body.String(),
		)
	}

	var state multiRaftNodeStatus

	if err := json.Unmarshal(
		response.Body.Bytes(),
		&state,
	); err != nil {
		t.Fatal(err)
	}

	if state.ID != 3 || state.Mode != "multi-raft" {
		t.Fatalf("unexpected status: %+v", state)
	}

	var groups []meta.ShardID

	for _, group := range state.Groups {
		groups = append(groups, group.ShardID)

		if group.Role != "follower" {
			t.Fatalf("unexpected group: %+v", group)
		}
	}

	if !reflect.DeepEqual(
		groups,
		[]meta.ShardID{1, 2, 3},
	) {
		t.Fatalf("incorrect groups: %v", groups)
	}

	t.Logf("node=3 local groups=%v", groups)

	response = httptest.NewRecorder()

	handler.ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodPost,
			"/status",
			nil,
		),
	)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status: %d", response.Code)
	}

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	response = httptest.NewRecorder()

	handler.ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodGet,
			"/status",
			nil,
		),
	)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d", response.Code)
	}
}

func TestMultiDaemonGRPCListener(t *testing.T) {
	manager := newMultiRaftTestManager(t)

	server, err := startMultiRaftGRPCServer(
		"127.0.0.1:0",
		manager,
		3,
	)

	if err != nil {
		t.Fatal(err)
	}

	defer server.Close()

	conn, err := grpc.NewClient(
		server.Addr().String(),
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()

	client := shardraftpb.NewRaftServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	response, err := client.Step(
		ctx,
		multiRaftTestRequest(2, 4, 3, 20),
	)

	if err != nil ||
		response == nil ||
		!response.Accepted {
		t.Fatalf(
			"response=%v err=%v",
			response,
			err,
		)
	}

	state := multiRaftTestStatus(t, manager, 2)

	if state.Term != 20 || state.LeaderID != 4 {
		t.Fatalf("incorrect group state: %+v", state)
	}

	t.Log("actual gRPC listener delivered message to shard 2")
}

func TestMultiDaemonLifecycle(t *testing.T) {
	cfg := multiDaemonTestConfig(t)

	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			150*time.Millisecond,
		)

		err := run(ctx, cfg)
		cancel()

		if err != nil {
			t.Fatalf(
				"startup/shutdown attempt=%d: %v",
				attempt+1,
				err,
			)
		}

		t.Logf(
			"startup/shutdown attempt=%d passed",
			attempt+1,
		)
	}
}
