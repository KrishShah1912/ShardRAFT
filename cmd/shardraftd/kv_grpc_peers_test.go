package main

import (
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

func TestKVGRPCPeerAddresses(t *testing.T) {
	tests := []struct {
		name  string
		peers []transport.Peer
		want  map[raft.NodeID]string
	}{
		{
			name:  "NoPeers",
			peers: nil,
			want:  map[raft.NodeID]string{},
		},
		{
			name: "ValidPeers",
			peers: []transport.Peer{
				{ID: 2, Address: "127.0.0.1:9092"},
				{ID: 3, Address: "127.0.0.1:9093"},
			},
			want: map[raft.NodeID]string{
				2: "127.0.0.1:9092",
				3: "127.0.0.1:9093",
			},
		},
		{
			name: "InvalidPeersIgnored",
			peers: []transport.Peer{
				{ID: 0, Address: "127.0.0.1:9091"},
				{ID: 2, Address: ""},
				{ID: 3, Address: "127.0.0.1:9093"},
			},
			want: map[raft.NodeID]string{
				3: "127.0.0.1:9093",
			},
		},
		{
			name: "IPv6Peer",
			peers: []transport.Peer{
				{ID: 2, Address: "[::1]:9092"},
			},
			want: map[raft.NodeID]string{
				2: "[::1]:9092",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := kvGRPCPeerAddresses(tt.peers)

			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf(
					"peer addresses = %v, want %v",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestKVGRPCPeerAddressesIndependentCopy(t *testing.T) {
	peers := []transport.Peer{
		{ID: 2, Address: "127.0.0.1:9092"},
	}

	addresses := kvGRPCPeerAddresses(peers)

	peers[0].Address = "127.0.0.1:9999"

	if addresses[2] != "127.0.0.1:9092" {
		t.Fatalf("peer address changed unexpectedly")
	}

	addresses[2] = "127.0.0.1:8888"

	if peers[0].Address != "127.0.0.1:9999" {
		t.Fatalf("original peer configuration was modified")
	}
}
