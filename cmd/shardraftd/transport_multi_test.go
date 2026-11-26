package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	"github.com/KrishShah1912/shardraft/internal/transport"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
)

type multiTransportReceived struct {
	nodeID raft.NodeID
	req    *shardraftpb.StepRequest
}

type multiTransportTestServer struct {
	shardraftpb.UnimplementedRaftServiceServer

	nodeID   raft.NodeID
	received chan multiTransportReceived
}

func (s *multiTransportTestServer) Step(
	ctx context.Context,
	req *shardraftpb.StepRequest,
) (*shardraftpb.StepResponse, error) {
	select {
	case s.received <- multiTransportReceived{
		nodeID: s.nodeID,
		req:    req,
	}:
		return &shardraftpb.StepResponse{
			Accepted: true,
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func startMultiTransportPeer(
	t *testing.T,
	id raft.NodeID,
	received chan multiTransportReceived,
) transport.Peer {
	t.Helper()

	listener, err := net.Listen(
		"tcp",
		"127.0.0.1:0",
	)

	if err != nil {
		t.Fatal(err)
	}

	server := grpc.NewServer()

	shardraftpb.RegisterRaftServiceServer(
		server,
		&multiTransportTestServer{
			nodeID:   id,
			received: received,
		},
	)

	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-done
	})

	return transport.Peer{
		ID:      id,
		Address: listener.Addr().String(),
	}
}

func TestMultiTransportGroupDelivery(t *testing.T) {
	received := make(
		chan multiTransportReceived, 16,
	)

	var peers []transport.Peer

	for _, id := range []raft.NodeID{1, 2, 4, 5} {
		peers = append(
			peers,
			startMultiTransportPeer(
				t, id, received,
			),
		)
	}

	sender, err := newMultiRaftTransport(
		3,
		meta.DefaultShardMap(),
		peers,
		16,
	)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}
	})

	if len(sender.groups) != 3 {
		t.Fatalf(
			"expected 3 local transports, got %d",
			len(sender.groups),
		)
	}

	type deliveryKey struct {
		group uint64
		node  raft.NodeID
	}

	expected := map[deliveryKey]bool{
		{1, 1}: true,
		{1, 2}: true,
		{2, 2}: true,
		{2, 4}: true,
		{3, 4}: true,
		{3, 5}: true,
	}

	for key := range expected {
		err := sender.Send(
			meta.ShardID(key.group),
			[]raft.Message{
				{
					Type: raft.MsgHeartbeat,
					From: 3,
					To:   key.node,
					Term: key.group * 10,
				},
			},
		)

		if err != nil {
			t.Fatal(err)
		}
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for len(expected) > 0 {
		select {
		case item := <-received:
			key := deliveryKey{
				group: item.req.ShardId,
				node:  item.nodeID,
			}

			if !expected[key] {
				t.Fatalf(
					"unexpected delivery: %+v",
					key,
				)
			}

			message, err :=
				transport.DecodeRaftMessage(
					item.req.Message,
				)

			if err != nil {
				t.Fatal(err)
			}

			if message.From != 3 ||
				message.To != key.node ||
				message.Term != key.group*10 ||
				message.Type != raft.MsgHeartbeat {
				t.Fatalf(
					"incorrect message: %+v",
					message,
				)
			}

			delete(expected, key)

			t.Logf(
				"shard=%d destination=%d term=%d verified",
				key.group,
				key.node,
				message.Term,
			)

		case <-timer.C:
			t.Fatalf(
				"missing deliveries: %v",
				expected,
			)
		}
	}

	t.Log("all six group-specific deliveries verified")
}

func TestMultiTransportRejectsInvalidRouting(t *testing.T) {
	peers := []transport.Peer{
		{ID: 1, Address: "127.0.0.1:19091"},
		{ID: 2, Address: "127.0.0.1:19092"},
		{ID: 4, Address: "127.0.0.1:19094"},
		{ID: 5, Address: "127.0.0.1:19095"},
	}

	sender, err := newMultiRaftTransport(
		3, meta.DefaultShardMap(), peers, 8,
	)

	if err != nil {
		t.Fatal(err)
	}

	defer sender.Close()

	message := raft.Message{
		Type: raft.MsgHeartbeat,
		From: 3,
		To:   1,
		Term: 1,
	}

	for _, id := range []meta.ShardID{0, 4, 99} {
		err := sender.Send(
			id, []raft.Message{message},
		)

		if !errors.Is(err, shard.ErrGroupNotLocal) {
			t.Fatalf(
				"group=%d unexpected error: %v",
				id, err,
			)
		}
	}

	wrongPeer := message
	wrongPeer.To = 5

	err = sender.Send(
		1, []raft.Message{wrongPeer},
	)

	if !errors.Is(err, transport.ErrPeer) {
		t.Fatalf("unexpected error: %v", err)
	}

	wrongSource := message
	wrongSource.From = 2

	if err := sender.Send(
		1, []raft.Message{wrongSource},
	); err == nil {
		t.Fatal("incorrect source accepted")
	}

	self := message
	self.To = 3

	if err := sender.Send(
		1, []raft.Message{self},
	); err == nil {
		t.Fatal("self message accepted")
	}

	for id, group := range sender.groups {
		if group.Stats().Enqueued != 0 {
			t.Fatalf(
				"invalid message enqueued in shard %d",
				id,
			)
		}
	}

	if err := sender.Close(); err != nil {
		t.Fatal(err)
	}

	if err := sender.Close(); err != nil {
		t.Fatal(err)
	}

	err = sender.Send(
		1, []raft.Message{message},
	)

	if !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMultiTransportInvalidConfiguration(t *testing.T) {
	peers := []transport.Peer{
		{ID: 1, Address: "127.0.0.1:19091"},
		{ID: 2, Address: "127.0.0.1:19092"},
		{ID: 4, Address: "127.0.0.1:19094"},
		{ID: 5, Address: "127.0.0.1:19095"},
	}

	cases := []struct {
		name  string
		node  raft.NodeID
		peers []transport.Peer
		queue int
	}{
		{"zero-node", 0, peers, 8},
		{"unknown-node", 99, peers, 8},
		{"zero-queue", 3, peers, 0},
		{"missing-peers", 3, nil, 8},
		{"partial-peers", 3, peers[:2], 8},
		{
			"duplicate-peer",
			3,
			append(
				append([]transport.Peer(nil), peers...),
				peers[0],
			),
			8,
		},
		{
			"self-peer",
			3,
			[]transport.Peer{
				{ID: 3, Address: "127.0.0.1:19093"},
			},
			8,
		},
		{
			"empty-address",
			3,
			[]transport.Peer{{ID: 1}},
			8,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sender, err := newMultiRaftTransport(
				tc.node,
				meta.DefaultShardMap(),
				tc.peers,
				tc.queue,
			)

			if err == nil {
				_ = sender.Close()
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
