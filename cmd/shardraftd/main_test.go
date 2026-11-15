package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParsePeers(t *testing.T) {
	peers, err := parsePeers("2, 3, 4")
	if err != nil {
		t.Fatal(err)
	}

	if len(peers) != 3 ||
		peers[0] != 2 ||
		peers[1] != 3 ||
		peers[2] != 4 {
		t.Fatalf("unexpected peers: %v", peers)
	}
}

func TestParsePeersRejectsInvalidIDs(t *testing.T) {
	cases := []string{
		"0",
		"abc",
		"2,2",
		"2,",
		"-1",
	}

	for _, value := range cases {
		t.Run(value, func(t *testing.T) {
			if _, err := parsePeers(value); err == nil {
				t.Fatalf("expected error for %q", value)
			}
		})
	}
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name string
		cfg  config
	}{
		{
			name: "zero node ID",
			cfg: config{
				id:              0,
				dataDir:         t.TempDir(),
				electionTimeout: 5,
				tickInterval:    time.Millisecond,
			},
		},
		{
			name: "empty data directory",
			cfg: config{
				id:              1,
				electionTimeout: 5,
				tickInterval:    time.Millisecond,
			},
		},
		{
			name: "invalid election timeout",
			cfg: config{
				id:           1,
				dataDir:      t.TempDir(),
				tickInterval: time.Millisecond,
			},
		},
		{
			name: "invalid tick interval",
			cfg: config{
				id:              1,
				dataDir:         t.TempDir(),
				electionTimeout: 5,
			},
		},
		{
			name: "self in peer list",
			cfg: config{
				id:              1,
				peers:           "1,2",
				dataDir:         t.TempDir(),
				electionTimeout: 5,
				tickInterval:    time.Millisecond,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := run(context.Background(), tc.cfg); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestDaemonGracefulShutdownAndRestart(t *testing.T) {
	dir := t.TempDir()

	cfg := config{
		id:              1,
		dataDir:         dir,
		electionTimeout: 5,
		tickInterval:    time.Millisecond,
	}

	for attempt := 1; attempt <= 2; attempt++ {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			30*time.Millisecond,
		)

		err := run(ctx, cfg)
		cancel()

		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
}

func TestMultiNodeTransportFailsClosed(t *testing.T) {
	cfg := config{
		id:              1,
		peers:           "2,3",
		dataDir:         t.TempDir(),
		electionTimeout: 2,
		tickInterval:    time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		500*time.Millisecond,
	)
	defer cancel()

	err := run(ctx, cfg)
	if err == nil {
		t.Fatal("expected transport error")
	}

	if !strings.Contains(err.Error(), "transport") {
		t.Fatalf("unexpected error: %v", err)
	}
}
