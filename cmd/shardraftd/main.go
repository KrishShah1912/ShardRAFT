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

	"github.com/KrishShah1912/shardraft/internal/observability"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

type config struct {
	multiRaft       bool
	id              uint64
	peers           string
	peerAddrs       string
	dataDir         string
	electionTimeout int
	tickInterval    time.Duration
	httpAddr        string
	raftTransport   string
	grpcAddr        string
	grpcPeerAddrs   string
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
	if cfg.multiRaft {
		return runMultiRaft(ctx, cfg)
	}

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

	transportCfg, err := resolveDaemonTransportConfig(
		cfg,
		peers,
	)
	if err != nil {
		return fmt.Errorf(
			"invalid transport configuration: %w",
			err,
		)
	}

	configuredPeers := transportCfg.httpPeers

	sender, err := createDaemonTransport(
		transportCfg,
	)
	if err != nil {
		return fmt.Errorf(
			"create transport: %w",
			err,
		)
	}

	if sender != nil {
		defer sender.Close()
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

	events := observability.NewBus(
		observability.DefaultHistorySize,
	)

	connectTransportObservability(
		sender,
		events,
		raft.NodeID(cfg.id),
	)

	node, err := shard.Open(shard.Config{
		ID:              raft.NodeID(cfg.id),
		Peers:           peers,
		ElectionTimeout: cfg.electionTimeout,
		WALPath:         walPath,
		SnapshotPath:    snapshotPath,
		Send:            send,
		Events:          events,
	})
	if err != nil {
		return fmt.Errorf("open shard: %w", err)
	}
	defer node.Close()

	var grpcErrors <-chan error

	if transportCfg.grpcAddr != "" {
		grpcServer, err := startDaemonGRPCServer(
			transportCfg.grpcAddr,
			node,
			peers,
			kvGRPCPeerAddresses(transportCfg.grpcPeers),
		)
		if err != nil {
			return err
		}

		defer grpcServer.Close()

		grpcErrors = grpcServer.Errors()

		log.Printf(
			"gRPC server listening on %s",
			grpcServer.Addr(),
		)
	}

	var server *http.Server
	var listener net.Listener
	var serverErrors chan error
	var serverDone chan struct{}

	if cfg.httpAddr != "" {
		listener, err = net.Listen("tcp", cfg.httpAddr)
		if err != nil {
			return fmt.Errorf(
				"start HTTP listener: %w",
				err,
			)
		}

		mux := http.NewServeMux()

		discovery := newLeaderDiscovery(
			raft.NodeID(cfg.id),
			configuredPeers,
			node.Status,
		)

		router := newRequestRouter(
			statusHandler(node),
			discovery.discover,
			node.Status,
		)

		router.verifyLeadership = len(configuredPeers) > 0

		mux.Handle("/", router)

		registerRaftHandlers(mux, node)
		registerAdminHandlers(
			mux,
			node,
			events,
			sender,
		)

		server = &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}

		serverErrors = make(chan error, 1)
		serverDone = make(chan struct{})

		go func() {
			defer close(serverDone)

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

			<-serverDone
		}()
	}

	var transportErrors <-chan error
	if sender != nil {
		transportErrors = sender.Errors()
	}

	log.Printf(
		"ShardRAFT node %d started (peers=%v, data=%s, transport=%s)",
		cfg.id,
		peers,
		cfg.dataDir,
		transportCfg.mode,
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

		case err := <-grpcErrors:
			if err != nil {
				return fmt.Errorf(
					"gRPC server failed: %w",
					err,
				)
			}

			return errors.New(
				"gRPC server stopped unexpectedly",
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

	flag.StringVar(
		&cfg.raftTransport,
		"raft-transport",
		"http",
		"Raft peer transport: http or grpc",
	)

	flag.StringVar(
		&cfg.grpcAddr,
		"grpc-addr",
		"",
		"gRPC server listen address",
	)

	flag.StringVar(
		&cfg.grpcPeerAddrs,
		"grpc-peer-addrs",
		"",
		"gRPC peer addresses: 2=127.0.0.1:9092,3=127.0.0.1:9093",
	)

	flag.BoolVar(
		&cfg.multiRaft,
		"multi-raft",
		false,
		"enable physical multi-Raft data groups",
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
