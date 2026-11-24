package main

import (
	"fmt"
	"net"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc"
)

const grpcShutdownTimeout = 5 * time.Second

type daemonGRPCServer struct {
	server   *grpc.Server
	listener net.Listener
	done     chan struct{}
	errors   chan error
}

func startDaemonGRPCServer(
	address string,
	node raftMessageHandler,
	peers []raft.NodeID,
) (*daemonGRPCServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf(
			"start gRPC listener: %w",
			err,
		)
	}

	server := grpc.NewServer()

	shardraftpb.RegisterRaftServiceServer(
		server,
		newRaftGRPCServer(
			defaultShardID,
			node,
			peers,
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

func (s *daemonGRPCServer) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *daemonGRPCServer) Errors() <-chan error {
	return s.errors
}

func (s *daemonGRPCServer) Close() {
	stopped := make(chan struct{})

	go func() {
		s.server.GracefulStop()
		close(stopped)
	}()

	timer := time.NewTimer(grpcShutdownTimeout)
	defer timer.Stop()

	select {
	case <-stopped:
	case <-timer.C:
		s.server.Stop()
		<-stopped
	}

	<-s.done
}
