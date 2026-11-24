package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

func newDiscoveryTestPeer(
	t *testing.T,
	status peerStatusResponse,
) transport.Peer {
	t.Helper()

	server := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			if r.URL.Path != "/status" {
				http.NotFound(w, r)
				return
			}

			w.Header().Set(
				"Content-Type",
				"application/json",
			)

			_ = json.NewEncoder(w).Encode(status)
		}),
	)

	t.Cleanup(server.Close)

	return transport.Peer{
		ID: status.ID,
		Address: strings.TrimPrefix(
			server.URL, "http://",
		),
	}
}

func TestLeaderDiscoveryLocalLeader(t *testing.T) {
	d := newLeaderDiscovery(
		1,
		nil,
		func() (raft.Status, error) {
			return raft.Status{
				ID:       1,
				Role:     raft.Leader,
				Term:     3,
				LeaderID: 1,
			}, nil
		},
	)

	target, err := d.discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if target.ID != 1 ||
		!target.Local ||
		target.Term != 3 {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestLeaderDiscoveryRemoteLeader(t *testing.T) {
	peer := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       2,
			Role:     raft.Leader,
			Term:     5,
			LeaderID: 2,
		},
	)

	d := newLeaderDiscovery(
		1,
		[]transport.Peer{peer},
		func() (raft.Status, error) {
			return raft.Status{
				ID:       1,
				Role:     raft.Follower,
				Term:     5,
				LeaderID: 2,
			}, nil
		},
	)

	target, err := d.discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if target.ID != 2 ||
		target.Local ||
		target.Term != 5 ||
		target.Address != peer.Address {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestLeaderDiscoveryHighestTerm(t *testing.T) {
	oldLeader := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       2,
			Role:     raft.Leader,
			Term:     4,
			LeaderID: 2,
		},
	)

	newLeader := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       3,
			Role:     raft.Leader,
			Term:     5,
			LeaderID: 3,
		},
	)

	d := newLeaderDiscovery(
		1,
		[]transport.Peer{oldLeader, newLeader},
		func() (raft.Status, error) {
			return raft.Status{
				ID:   1,
				Role: raft.Follower,
				Term: 4,
			}, nil
		},
	)

	target, err := d.discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if target.ID != 3 || target.Term != 5 {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestLeaderDiscoveryUnavailable(t *testing.T) {
	d := newLeaderDiscovery(
		1,
		nil,
		func() (raft.Status, error) {
			return raft.Status{
				ID:   1,
				Role: raft.Candidate,
				Term: 3,
			}, nil
		},
	)

	_, err := d.discover(context.Background())

	if !errors.Is(err, errLeaderUnavailable) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLeaderDiscoveryRejectsStaleLeader(t *testing.T) {
	peer := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       2,
			Role:     raft.Leader,
			Term:     4,
			LeaderID: 2,
		},
	)

	d := newLeaderDiscovery(
		1,
		[]transport.Peer{peer},
		func() (raft.Status, error) {
			return raft.Status{
				ID:   1,
				Role: raft.Candidate,
				Term: 5,
			}, nil
		},
	)

	_, err := d.discover(context.Background())

	if !errors.Is(err, errLeaderUnavailable) {
		t.Fatalf("expected unavailable, got %v", err)
	}
}

func TestLeaderDiscoveryConflictingLeaders(t *testing.T) {
	first := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       2,
			Role:     raft.Leader,
			Term:     5,
			LeaderID: 2,
		},
	)

	second := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       3,
			Role:     raft.Leader,
			Term:     5,
			LeaderID: 3,
		},
	)

	d := newLeaderDiscovery(
		1,
		[]transport.Peer{first, second},
		func() (raft.Status, error) {
			return raft.Status{
				ID:   1,
				Role: raft.Follower,
				Term: 5,
			}, nil
		},
	)

	_, err := d.discover(context.Background())

	if !errors.Is(err, errLeaderUnavailable) {
		t.Fatalf("expected unavailable, got %v", err)
	}
}

func TestLeaderDiscoveryCanceledContext(t *testing.T) {
	d := newLeaderDiscovery(
		1,
		nil,
		func() (raft.Status, error) {
			return raft.Status{}, nil
		},
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, err := d.discover(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
}
