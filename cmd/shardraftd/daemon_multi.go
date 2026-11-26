package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KrishShah1912/shardraft/internal/observability"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
)

func validateMultiRaftConfig(cfg config) error {
	if cfg.id < 1 || cfg.id > 5 {
		return errors.New(
			"multi-Raft node ID must be between 1 and 5",
		)
	}

	if cfg.dataDir == "" {
		return errors.New("data directory is required")
	}

	if cfg.electionTimeout <= 0 {
		return errors.New("invalid election timeout")
	}

	if cfg.tickInterval <= 0 {
		return errors.New("invalid tick interval")
	}

	if cfg.raftTransport != raftTransportGRPC {
		return errors.New(
			"multi-Raft requires gRPC transport",
		)
	}

	if cfg.grpcAddr == "" {
		return errors.New(
			"gRPC listen address is required",
		)
	}

	peers, err := parsePeers(cfg.peers)
	if err != nil {
		return err
	}

	if len(peers) != 4 {
		return errors.New(
			"multi-Raft requires four remote storage nodes",
		)
	}

	for _, peer := range peers {
		if peer < 1 ||
			peer > 5 ||
			peer == raft.NodeID(cfg.id) {
			return fmt.Errorf(
				"invalid remote node %d", peer,
			)
		}
	}

	_, err = validatePeerAddresses(
		cfg.grpcPeerAddrs,
		peers,
		true,
	)

	return err
}

func startMultiRaftGRPCServer(
	address string,
	manager *shard.Manager,
	nodeID raft.NodeID,
	metadata ...*meta.Group,
) (*daemonGRPCServer, error) {
	return startMultiRaftGRPCServerWithAddresses(
		address,
		manager,
		nodeID,
		nil,
		metadata...,
	)
}

func startMultiRaftGRPCServerWithAddresses(
	address string,
	manager *shard.Manager,
	nodeID raft.NodeID,
	peerAddresses map[raft.NodeID]string,
	metadata ...*meta.Group,
) (*daemonGRPCServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf(
			"start multi-Raft gRPC listener: %w",
			err,
		)
	}

	server := grpc.NewServer()

	handler := newMultiRaftGRPCServer(
		manager,
		nodeID,
	)

	if len(metadata) > 0 {
		handler.metadata = metadata[0]
	}

	shardraftpb.RegisterRaftServiceServer(
		server,
		handler,
	)

	addresses := make(map[raft.NodeID]string)

	for id, endpoint := range peerAddresses {
		addresses[id] = endpoint
	}

	addresses[nodeID] = metaGRPCLocalAddress(
		address,
		listener.Addr(),
	)

	shardraftpb.RegisterMetaServiceServer(
		server,
		newMultiMetaGRPCServer(
			manager,
			handler.metadata,
			addresses,
		),
	)

	shardraftpb.RegisterKVServiceServer(
		server,
		newMultiKVGRPCServer(
			manager,
			addresses,
		),
	)

	result := &daemonGRPCServer{
		server:   server,
		listener: listener,
		done:     make(chan struct{}),
		errors:   make(chan error, 1),
	}

	go func() {
		defer close(result.done)
		result.errors <- server.Serve(listener)
	}()

	return result, nil
}

type multiRaftGroupStatus struct {
	ShardID      meta.ShardID `json:"shard_id"`
	Role         string       `json:"role"`
	Term         uint64       `json:"term"`
	LeaderID     raft.NodeID  `json:"leader_id"`
	CommitIndex  uint64       `json:"commit_index"`
	AppliedIndex uint64       `json:"applied_index"`
}

type multiRaftNodeStatus struct {
	ID     raft.NodeID            `json:"id"`
	Mode   string                 `json:"mode"`
	Groups []multiRaftGroupStatus `json:"groups"`
}

func multiRaftStatusHandler(
	manager *shard.Manager,
	nodeID raft.NodeID,
) http.HandlerFunc {
	return func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(
				w,
				"method not allowed",
				http.StatusMethodNotAllowed,
			)
			return
		}

		result := multiRaftNodeStatus{
			ID:   nodeID,
			Mode: "multi-raft",
		}

		for _, id := range manager.GroupIDs() {
			group, err := manager.Group(id)
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			state, applied, err :=
				group.Shard().StatusSnapshot()

			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			role := "unknown"

			switch state.Role {
			case raft.Follower:
				role = "follower"
			case raft.Candidate:
				role = "candidate"
			case raft.Leader:
				role = "leader"
			}

			result.Groups = append(
				result.Groups,
				multiRaftGroupStatus{
					ShardID:      id,
					Role:         role,
					Term:         state.Term,
					LeaderID:     state.LeaderID,
					CommitIndex:  state.Commit,
					AppliedIndex: applied,
				},
			)
		}

		if len(result.Groups) == 0 {
			http.Error(
				w,
				"no available local groups",
				http.StatusServiceUnavailable,
			)
			return
		}

		w.Header().Set(
			"Content-Type",
			"application/json",
		)

		w.Header().Set(
			"Cache-Control",
			"no-store",
		)

		_ = json.NewEncoder(w).Encode(result)
	}
}

func runMultiRaft(
	ctx context.Context,
	cfg config,
) (result error) {
	if err := validateMultiRaftConfig(cfg); err != nil {
		return err
	}

	peers, err := parsePeers(cfg.peers)
	if err != nil {
		return err
	}

	remote, err := validatePeerAddresses(
		cfg.grpcPeerAddrs,
		peers,
		true,
	)
	if err != nil {
		return err
	}

	layout := meta.DefaultShardMap()
	nodeID := raft.NodeID(cfg.id)

	sender, err := newMultiRaftTransport(
		nodeID,
		layout,
		remote,
		transportQueueSize,
	)

	if err != nil {
		return err
	}

	defer func() {
		result = errors.Join(
			result,
			sender.Close(),
		)
	}()

	events := observability.NewBus(observability.DefaultHistorySize)
	connectMultiTransportEvents(sender, events, nodeID)

	manager, err := shard.OpenManager(
		shard.ManagerConfig{
			Events:          events,
			NodeID:          nodeID,
			DataDir:         cfg.dataDir,
			ElectionTimeout: cfg.electionTimeout,
			ShardMap:        layout,
			Send:            sender.Send,
		},
	)

	if err != nil {
		return err
	}

	defer func() {
		result = errors.Join(
			result,
			manager.Close(),
		)
	}()

	metadata, err := openMetadataRuntime(
		cfg,
		remote,
	)

	if err != nil {
		return err
	}

	defer func() {
		result = errors.Join(
			result,
			metadata.Close(),
		)
	}()

	discoveryAddresses := make(map[raft.NodeID]string)

	for _, peer := range remote {
		discoveryAddresses[peer.ID] = peer.Address
	}

	grpcServer, err := startMultiRaftGRPCServerWithAddresses(
		cfg.grpcAddr,
		manager,
		nodeID,
		discoveryAddresses,
		metadata.Group(),
	)

	if err != nil {
		return err
	}

	defer grpcServer.Close()

	var httpErrors <-chan error

	if cfg.httpAddr != "" {
		listener, err := net.Listen(
			"tcp",
			cfg.httpAddr,
		)

		if err != nil {
			return err
		}

		mux := http.NewServeMux()

		mux.Handle(
			"/status",
			multiRaftStatusHandler(
				manager,
				nodeID,
			),
		)

		discoveryAddresses[nodeID] = metaGRPCLocalAddress(
			cfg.grpcAddr,
			grpcServer.listener.Addr(),
		)

		registerMultiMetadataHTTP(
			mux,
			metadata.Group(),
			newMultiMetaGRPCServer(
				manager,
				metadata.Group(),
				discoveryAddresses,
			),
			nodeID,
		)

		registerMultiRaftAdminHTTP(
			mux,
			manager,
			nodeID,
		)

		registerMultiTransportHTTP(
			mux,
			sender,
			nodeID,
		)

		registerEventHandlers(mux, events)

		server := &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}

		serverErrors := make(chan error, 1)
		serverDone := make(chan struct{})

		httpErrors = serverErrors

		go func() {
			defer close(serverDone)
			serverErrors <- server.Serve(listener)
		}()

		defer func() {
			shutdownCtx, cancel :=
				context.WithTimeout(
					context.Background(),
					5*time.Second,
				)

			defer cancel()

			if err := server.Shutdown(
				shutdownCtx,
			); err != nil {
				_ = server.Close()
				result = errors.Join(
					result, err,
				)
			}

			<-serverDone
		}()

		log.Printf(
			"multi-Raft HTTP status listening on %s",
			listener.Addr(),
		)
	}

	log.Printf(
		"multi-Raft node %d started groups=%v grpc=%s",
		nodeID,
		manager.GroupIDs(),
		grpcServer.Addr(),
	)

	ticker := time.NewTicker(cfg.tickInterval)
	defer ticker.Stop()

	groupIDs := manager.GroupIDs()

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-grpcServer.Errors():
			if err == nil {
				return errors.New(
					"gRPC server stopped unexpectedly",
				)
			}

			return fmt.Errorf(
				"gRPC server failed: %w",
				err,
			)

		case err := <-httpErrors:
			if err == nil {
				return errors.New(
					"HTTP server stopped unexpectedly",
				)
			}

			return fmt.Errorf(
				"HTTP server failed: %w",
				err,
			)

		case <-ticker.C:
			if err := metadata.Tick(); err != nil {
				return fmt.Errorf(
					"tick metadata group: %w",
					err,
				)
			}

			for _, id := range groupIDs {
				if err := manager.Tick(id); err != nil {
					return fmt.Errorf(
						"tick shard %d: %w",
						id,
						err,
					)
				}
			}
		}
	}
}
