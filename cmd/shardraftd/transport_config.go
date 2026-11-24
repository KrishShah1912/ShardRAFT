package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

const (
	raftTransportHTTP  = "http"
	raftTransportGRPC  = "grpc"
	defaultShardID     = uint64(1)
	transportQueueSize = 256
)

type daemonTransportConfig struct {
	mode      string
	httpPeers []transport.Peer
	grpcPeers []transport.Peer
	grpcAddr  string
}

func validatePeerAddresses(
	value string,
	peers []raft.NodeID,
	required bool,
) ([]transport.Peer, error) {
	if strings.TrimSpace(value) == "" {
		if required && len(peers) > 0 {
			return nil, errors.New("peer addresses are required")
		}

		return nil, nil
	}

	if len(peers) == 0 {
		return nil, errors.New("peer addresses provided without peers")
	}

	configured, err := transport.ParsePeers(value)
	if err != nil {
		return nil, err
	}

	if len(configured) != len(peers) {
		return nil, errors.New(
			"peer address count does not match peer count",
		)
	}

	expected := make(map[raft.NodeID]bool, len(peers))

	for _, peer := range peers {
		expected[peer] = true
	}

	for _, peer := range configured {
		if !expected[peer.ID] {
			return nil, fmt.Errorf(
				"unexpected peer address ID %d",
				peer.ID,
			)
		}
	}

	return configured, nil
}

func resolveDaemonTransportConfig(
	cfg config,
	peers []raft.NodeID,
) (daemonTransportConfig, error) {
	var result daemonTransportConfig

	result.mode = strings.ToLower(
		strings.TrimSpace(cfg.raftTransport),
	)

	if result.mode == "" {
		result.mode = raftTransportHTTP
	}

	if result.mode != raftTransportHTTP &&
		result.mode != raftTransportGRPC {
		return result, fmt.Errorf(
			"unsupported Raft transport %q",
			result.mode,
		)
	}

	var err error

	result.httpPeers, err = validatePeerAddresses(
		cfg.peerAddrs,
		peers,
		false,
	)
	if err != nil {
		return result, fmt.Errorf(
			"parse HTTP peer addresses: %w",
			err,
		)
	}

	result.grpcAddr = strings.TrimSpace(cfg.grpcAddr)

	if result.mode == raftTransportHTTP {
		if strings.TrimSpace(cfg.grpcPeerAddrs) != "" {
			return result, errors.New(
				"gRPC peer addresses require gRPC transport mode",
			)
		}
	}

	if result.mode == raftTransportGRPC {
		if result.grpcAddr == "" {
			return result, errors.New(
				"gRPC listen address is required",
			)
		}

		result.grpcPeers, err = validatePeerAddresses(
			cfg.grpcPeerAddrs,
			peers,
			true,
		)
		if err != nil {
			return result, fmt.Errorf(
				"parse gRPC peer addresses: %w",
				err,
			)
		}

		if cfg.httpAddr != "" &&
			len(peers) > 0 &&
			len(result.httpPeers) != len(peers) {
			return result, errors.New(
				"HTTP peer addresses are required for HTTP request routing",
			)
		}
	}

	if result.grpcAddr != "" {
		_, port, err := net.SplitHostPort(result.grpcAddr)
		if err != nil {
			return result, fmt.Errorf(
				"invalid gRPC listen address: %w",
				err,
			)
		}

		number, err := strconv.Atoi(port)
		if err != nil || number < 0 || number > 65535 {
			return result, errors.New(
				"invalid gRPC listen port",
			)
		}

		if number != 0 && result.grpcAddr == cfg.httpAddr {
			return result, errors.New(
				"HTTP and gRPC listeners must use separate addresses",
			)
		}
	}

	return result, nil
}

func createDaemonTransport(
	cfg daemonTransportConfig,
) (*transport.HTTPTransport, error) {
	switch cfg.mode {
	case raftTransportHTTP:
		if len(cfg.httpPeers) == 0 {
			return nil, nil
		}

		return transport.NewHTTPTransport(
			cfg.httpPeers,
			transportQueueSize,
		)

	case raftTransportGRPC:
		if len(cfg.grpcPeers) == 0 {
			return nil, nil
		}

		return transport.NewGRPCTransport(
			defaultShardID,
			cfg.grpcPeers,
			transportQueueSize,
		)

	default:
		return nil, fmt.Errorf(
			"unsupported Raft transport %q",
			cfg.mode,
		)
	}
}
