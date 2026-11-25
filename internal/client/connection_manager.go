package client

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrInvalidEndpoint         = errors.New("invalid gRPC endpoint")
	ErrConnectionManagerClosed = errors.New(
		"connection manager closed",
	)
)

// ConnectionManager maintains reusable gRPC connections.
//
// Connections are created lazily and shared across
// concurrent callers.
type ConnectionManager struct {
	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
	options     []grpc.DialOption
	closed      bool
}

// NewConnectionManager creates an empty connection manager.
//
// The default configuration uses insecure transport
// credentials for the existing local ShardRAFT deployment.
func NewConnectionManager(
	options ...grpc.DialOption,
) *ConnectionManager {
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	}

	dialOptions = append(dialOptions, options...)

	return &ConnectionManager{
		connections: make(map[string]*grpc.ClientConn),
		options:     dialOptions,
	}
}

// normalizeEndpoint validates a concrete host:port endpoint.
func normalizeEndpoint(address string) (string, error) {
	address = strings.TrimSpace(address)

	host, port, err := net.SplitHostPort(address)

	if err != nil {
		return "", fmt.Errorf(
			"%w: %q",
			ErrInvalidEndpoint,
			address,
		)
	}

	if host == "" ||
		host == "*" ||
		strings.ContainsAny(host, " \t\r\n/\\") {
		return "", fmt.Errorf(
			"%w: invalid host",
			ErrInvalidEndpoint,
		)
	}

	ipHost := host

	if index := strings.LastIndex(ipHost, "%"); index >= 0 {
		ipHost = ipHost[:index]
	}

	if ip := net.ParseIP(ipHost); ip != nil &&
		ip.IsUnspecified() {
		return "", fmt.Errorf(
			"%w: wildcard address",
			ErrInvalidEndpoint,
		)
	}

	number, err := strconv.Atoi(port)

	if err != nil || number <= 0 || number > 65535 {
		return "", fmt.Errorf(
			"%w: invalid port",
			ErrInvalidEndpoint,
		)
	}

	return net.JoinHostPort(
		host,
		strconv.Itoa(number),
	), nil
}

// Connection returns a reusable connection for an endpoint.
//
// Creating a ClientConn does not establish that the remote
// server is reachable. RPCs must use appropriate deadlines.
func (manager *ConnectionManager) Connection(
	address string,
) (*grpc.ClientConn, error) {
	endpoint, err := normalizeEndpoint(address)

	if err != nil {
		return nil, err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.closed {
		return nil, ErrConnectionManagerClosed
	}

	if conn, exists := manager.connections[endpoint]; exists {
		return conn, nil
	}

	conn, err := grpc.NewClient(
		"passthrough:///"+endpoint,
		manager.options...,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"create gRPC connection to %s: %w",
			endpoint,
			err,
		)
	}

	manager.connections[endpoint] = conn

	return conn, nil
}

// KVClient returns a KVService client backed by a
// reusable connection.
func (manager *ConnectionManager) KVClient(
	address string,
) (shardraftpb.KVServiceClient, error) {
	conn, err := manager.Connection(address)

	if err != nil {
		return nil, err
	}

	return shardraftpb.NewKVServiceClient(conn), nil
}

// MetaClient returns a MetaService client backed by a
// reusable connection.
func (manager *ConnectionManager) MetaClient(
	address string,
) (shardraftpb.MetaServiceClient, error) {
	conn, err := manager.Connection(address)

	if err != nil {
		return nil, err
	}

	return shardraftpb.NewMetaServiceClient(conn), nil
}

// Count returns the number of cached connections.
func (manager *ConnectionManager) Count() int {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	return len(manager.connections)
}

// Close releases all cached connections.
//
// Close is idempotent. New connections cannot be created
// after the manager has been closed.
func (manager *ConnectionManager) Close() error {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.closed {
		return nil
	}

	manager.closed = true

	var closeErrors []error

	for address, conn := range manager.connections {
		if err := conn.Close(); err != nil {
			closeErrors = append(
				closeErrors,
				fmt.Errorf(
					"close connection %s: %w",
					address,
					err,
				),
			)
		}

		delete(manager.connections, address)
	}

	return errors.Join(closeErrors...)
}
