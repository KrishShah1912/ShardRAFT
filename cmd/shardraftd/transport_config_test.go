package main

import (
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestDaemonTransportDefaultHTTP(t *testing.T) {
	result, err := resolveDaemonTransportConfig(
		config{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	if result.mode != raftTransportHTTP {
		t.Fatalf("unexpected mode: %s", result.mode)
	}
}

func TestDaemonTransportHTTPConfiguration(t *testing.T) {
	result, err := resolveDaemonTransportConfig(
		config{
			peerAddrs: "2=127.0.0.1:8082",
		},
		[]raft.NodeID{2},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.httpPeers) != 1 ||
		len(result.grpcPeers) != 0 {
		t.Fatalf("unexpected configuration: %+v", result)
	}

	sender, err := createDaemonTransport(result)
	if err != nil {
		t.Fatal(err)
	}

	if sender == nil {
		t.Fatal("HTTP transport was not created")
	}

	defer sender.Close()
}

func TestDaemonTransportGRPCConfiguration(t *testing.T) {
	result, err := resolveDaemonTransportConfig(
		config{
			raftTransport: "grpc",
			httpAddr:      "127.0.0.1:8081",
			grpcAddr:      "127.0.0.1:9091",
			peerAddrs:     "2=127.0.0.1:8082",
			grpcPeerAddrs: "2=127.0.0.1:9092",
		},
		[]raft.NodeID{2},
	)
	if err != nil {
		t.Fatal(err)
	}

	if result.mode != raftTransportGRPC {
		t.Fatal("expected gRPC transport mode")
	}

	if result.httpPeers[0].Address != "127.0.0.1:8082" {
		t.Fatal("incorrect HTTP peer address")
	}

	if result.grpcPeers[0].Address != "127.0.0.1:9092" {
		t.Fatal("incorrect gRPC peer address")
	}

	sender, err := createDaemonTransport(result)
	if err != nil {
		t.Fatal(err)
	}

	if sender == nil {
		t.Fatal("gRPC transport was not created")
	}

	defer sender.Close()

	stats := sender.Stats()

	if len(stats.Peers) != 1 ||
		stats.Peers[0].Address != "127.0.0.1:9092" {
		t.Fatalf("incorrect transport peers: %+v", stats)
	}
}

func TestDaemonTransportSingleNodeGRPC(t *testing.T) {
	result, err := resolveDaemonTransportConfig(
		config{
			raftTransport: "grpc",
			grpcAddr:      "127.0.0.1:0",
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	sender, err := createDaemonTransport(result)
	if err != nil {
		t.Fatal(err)
	}

	if sender != nil {
		sender.Close()
		t.Fatal("single node does not require a peer sender")
	}
}

func TestDaemonTransportInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		cfg   config
		peers []raft.NodeID
	}{
		{
			name: "unsupported transport",
			cfg: config{
				raftTransport: "udp",
			},
		},
		{
			name: "missing gRPC listener",
			cfg: config{
				raftTransport: "grpc",
			},
		},
		{
			name: "missing gRPC peer addresses",
			cfg: config{
				raftTransport: "grpc",
				grpcAddr:      "127.0.0.1:9091",
			},
			peers: []raft.NodeID{2},
		},
		{
			name: "wrong gRPC peer ID",
			cfg: config{
				raftTransport: "grpc",
				grpcAddr:      "127.0.0.1:9091",
				grpcPeerAddrs: "3=127.0.0.1:9093",
			},
			peers: []raft.NodeID{2},
		},
		{
			name: "duplicate gRPC peers",
			cfg: config{
				raftTransport: "grpc",
				grpcAddr:      "127.0.0.1:9091",
				grpcPeerAddrs: "2=127.0.0.1:9092,2=127.0.0.1:9093",
			},
			peers: []raft.NodeID{2, 3},
		},
		{
			name: "invalid gRPC listener",
			cfg: config{
				raftTransport: "grpc",
				grpcAddr:      "invalid-address",
			},
		},
		{
			name: "invalid gRPC port",
			cfg: config{
				raftTransport: "grpc",
				grpcAddr:      "127.0.0.1:70000",
			},
		},
		{
			name: "same HTTP and gRPC listener",
			cfg: config{
				raftTransport: "grpc",
				httpAddr:      "127.0.0.1:8081",
				grpcAddr:      "127.0.0.1:8081",
			},
		},
		{
			name: "gRPC peers in HTTP mode",
			cfg: config{
				raftTransport: "http",
				grpcPeerAddrs: "2=127.0.0.1:9092",
			},
			peers: []raft.NodeID{2},
		},
		{
			name: "missing HTTP routing addresses",
			cfg: config{
				raftTransport: "grpc",
				httpAddr:      "127.0.0.1:8081",
				grpcAddr:      "127.0.0.1:9091",
				grpcPeerAddrs: "2=127.0.0.1:9092",
			},
			peers: []raft.NodeID{2},
		},
		{
			name: "addresses without peers",
			cfg: config{
				peerAddrs: "2=127.0.0.1:8082",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveDaemonTransportConfig(
				tc.cfg,
				tc.peers,
			)

			if err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestDaemonTransportUnknownConstructorMode(t *testing.T) {
	_, err := createDaemonTransport(
		daemonTransportConfig{
			mode: "unknown",
		},
	)

	if err == nil {
		t.Fatal("expected unsupported transport error")
	}
}
