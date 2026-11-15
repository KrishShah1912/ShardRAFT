package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

type config struct {
	id              uint64
	peers           string
	peerAddrs       string
	dataDir         string
	electionTimeout int
	tickInterval    time.Duration
	httpAddr        string
}

func parsePeers(value string) ([]raft.NodeID, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}

	var peers []raft.NodeID
	seen := make(map[raft.NodeID]bool)

	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)

		id, err := strconv.ParseUint(item, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid peer ID %q", item)
		}

		peer := raft.NodeID(id)
		if seen[peer] {
			return nil, fmt.Errorf("duplicate peer ID %d", peer)
		}

		seen[peer] = true
		peers = append(peers, peer)
	}

	return peers, nil
}

func run(ctx context.Context, cfg config) error {
	if cfg.id == 0 {
		return errors.New("node ID must be nonzero")
	}
	if cfg.dataDir == "" {
		return errors.New("data directory is required")
	}
	if cfg.electionTimeout <= 0 {
		return errors.New("election timeout must be positive")
	}
	if cfg.tickInterval <= 0 {
		return errors.New("tick interval must be positive")
	}

	peers, err := parsePeers(cfg.peers)
	if err != nil {
		return err
	}

	for _, peer := range peers {
		if peer == raft.NodeID(cfg.id) {
			return errors.New("peer list must exclude local node ID")
		}
	}

	var sender *transport.HTTPTransport

	if len(peers) > 0 && cfg.peerAddrs != "" {
		configuredPeers, err := transport.ParsePeers(cfg.peerAddrs)
		if err != nil {
			return fmt.Errorf("parse peer addresses: %w", err)
		}

		if len(configuredPeers) != len(peers) {
			return errors.New(
				"peer address count does not match peer count",
			)
		}

		expected := make(map[raft.NodeID]bool)
		for _, peer := range peers {
			expected[peer] = true
		}

		for _, peer := range configuredPeers {
			if !expected[peer.ID] {
				return fmt.Errorf(
					"unexpected peer address ID %d",
					peer.ID,
				)
			}
		}

		sender, err = transport.NewHTTPTransport(
			configuredPeers,
			256,
		)
		if err != nil {
			return fmt.Errorf("create transport: %w", err)
		}
		defer sender.Close()
	} else if len(peers) == 0 && cfg.peerAddrs != "" {
		return errors.New(
			"peer addresses provided without peers",
		)
	}

	if err := os.MkdirAll(cfg.dataDir, 0700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	walPath := filepath.Join(cfg.dataDir, "raft.wal")
	snapshotPath := filepath.Join(cfg.dataDir, "raft.snapshot")

	send := func(messages []raft.Message) error {
		if sender == nil {
			if len(messages) > 0 && len(peers) > 0 {
				return errors.New(
					"multi-node transport is not configured",
				)
			}
			return nil
		}

		return sendRaftMessages(sender, messages)
	}

	node, err := shard.Open(shard.Config{
		ID:              raft.NodeID(cfg.id),
		Peers:           peers,
		ElectionTimeout: cfg.electionTimeout,
		WALPath:         walPath,
		SnapshotPath:    snapshotPath,
		Send:            send,
	})
	if err != nil {
		return fmt.Errorf("open shard: %w", err)
	}
	defer node.Close()

	var server *http.Server
	var listener net.Listener
	var serverErrors chan error

	if cfg.httpAddr != "" {
		listener, err = net.Listen("tcp", cfg.httpAddr)
		if err != nil {
			return fmt.Errorf(
				"start HTTP listener: %w",
				err,
			)
		}

		mux := http.NewServeMux()
		mux.Handle("/", statusHandler(node))
		registerRaftHandlers(mux, node)

		server = &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}

		serverErrors = make(chan error, 1)

		go func() {
			err := server.Serve(listener)
			if err != nil &&
				!errors.Is(err, http.ErrServerClosed) {
				serverErrors <- err
				return
			}
			serverErrors <- nil
		}()

		log.Printf(
			"HTTP status listening on %s",
			listener.Addr(),
		)
	}

	if server != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancel()

			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Printf("HTTP shutdown error: %v", err)
				_ = server.Close()
			}

			<-serverErrors
		}()
	}

	var transportErrors <-chan error
	if sender != nil {
		transportErrors = sender.Errors()
	}

	log.Printf(
		"ShardRAFT node %d started (peers=%v, data=%s)",
		cfg.id,
		peers,
		cfg.dataDir,
	)

	ticker := time.NewTicker(cfg.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf(
				"ShardRAFT node %d shutting down",
				cfg.id,
			)
			return nil

		case err := <-serverErrors:
			if err != nil {
				return fmt.Errorf(
					"HTTP server failed: %w",
					err,
				)
			}
			return errors.New(
				"HTTP server stopped unexpectedly",
			)

		case err := <-transportErrors:
			log.Printf(
				"Raft transport delivery failure: %v",
				err,
			)

		case <-ticker.C:
			if err := node.Tick(); err != nil {
				return fmt.Errorf(
					"Raft tick failed: %w",
					err,
				)
			}
		}
	}
}

func main() {
	var cfg config

	flag.Uint64Var(
		&cfg.id,
		"id",
		1,
		"Raft node ID",
	)

	flag.StringVar(
		&cfg.peers,
		"peers",
		"",
		"comma-separated peer node IDs",
	)

	flag.StringVar(
		&cfg.peerAddrs,
		"peer-addrs",
		"",
		"peer addresses: 2=127.0.0.1:8082,3=127.0.0.1:8083",
	)

	flag.StringVar(
		&cfg.dataDir,
		"data-dir",
		"./data/node-1",
		"durable data directory",
	)

	flag.IntVar(
		&cfg.electionTimeout,
		"election-timeout",
		10,
		"election timeout in ticks",
	)

	flag.DurationVar(
		&cfg.tickInterval,
		"tick-interval",
		100*time.Millisecond,
		"Raft tick interval",
	)

	flag.StringVar(
		&cfg.httpAddr,
		"http-addr",
		"127.0.0.1:8080",
		"HTTP status listen address",
	)

	flag.Parse()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
