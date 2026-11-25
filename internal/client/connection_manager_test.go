package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

func newTestConnectionManager(
	t testing.TB,
) *ConnectionManager {
	t.Helper()

	manager := NewConnectionManager()

	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})

	return manager
}

func TestConnectionManagerValidEndpoints(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"127.0.0.1:9091", "127.0.0.1:9091"},
		{"localhost:9091", "localhost:9091"},
		{"node.example.com:443", "node.example.com:443"},
		{"[::1]:9091", "[::1]:9091"},
		{"[fe80::1%en0]:9091", "[fe80::1%en0]:9091"},
		{" 127.0.0.1:9091 ", "127.0.0.1:9091"},
		{"localhost:09091", "localhost:9091"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := normalizeEndpoint(tt.input)

			if err != nil || got != tt.want {
				t.Fatalf(
					"endpoint=%q want=%q err=%v",
					got,
					tt.want,
					err,
				)
			}
		})
	}
}

func TestConnectionManagerInvalidEndpoints(t *testing.T) {
	addresses := []string{
		"",
		" ",
		"localhost",
		"localhost:",
		":9091",
		"localhost:0",
		"localhost:-1",
		"localhost:65536",
		"localhost:abc",
		"0.0.0.0:9091",
		"[::]:9091",
		"*:9091",
		"bad host:9091",
		"http://localhost:9091",
		"bad/host:9091",
		"bad\\host:9091",
	}

	manager := newTestConnectionManager(t)

	for _, address := range addresses {
		t.Run(address, func(t *testing.T) {
			conn, err := manager.Connection(address)

			if conn != nil ||
				!errors.Is(err, ErrInvalidEndpoint) {
				t.Fatalf(
					"conn=%v err=%v",
					conn,
					err,
				)
			}
		})
	}

	if manager.Count() != 0 {
		t.Fatal("invalid endpoints created connections")
	}
}

func TestConnectionManagerInitiallyEmpty(t *testing.T) {
	manager := newTestConnectionManager(t)

	if manager.Count() != 0 {
		t.Fatal("new manager contains connections")
	}
}

func TestConnectionManagerReuse(t *testing.T) {
	manager := newTestConnectionManager(t)

	first, err := manager.Connection("127.0.0.1:9091")

	if err != nil {
		t.Fatal(err)
	}

	second, err := manager.Connection("127.0.0.1:9091")

	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatal("connection was not reused")
	}

	if manager.Count() != 1 {
		t.Fatal("unexpected connection count")
	}
}

func TestConnectionManagerNormalizedReuse(t *testing.T) {
	manager := newTestConnectionManager(t)

	first, err := manager.Connection("localhost:9091")

	if err != nil {
		t.Fatal(err)
	}

	second, err := manager.Connection(" localhost:09091 ")

	if err != nil {
		t.Fatal(err)
	}

	if first != second || manager.Count() != 1 {
		t.Fatal("normalized endpoint was not reused")
	}
}

func TestConnectionManagerDistinctEndpoints(t *testing.T) {
	manager := newTestConnectionManager(t)

	first, err := manager.Connection("127.0.0.1:9091")

	if err != nil {
		t.Fatal(err)
	}

	second, err := manager.Connection("127.0.0.1:9092")

	if err != nil {
		t.Fatal(err)
	}

	if first == second || manager.Count() != 2 {
		t.Fatal("distinct endpoints shared a connection")
	}
}

func TestConnectionManagerServiceClients(t *testing.T) {
	manager := newTestConnectionManager(t)

	kv, err := manager.KVClient("127.0.0.1:9091")

	if err != nil || kv == nil {
		t.Fatalf("KV client=%v err=%v", kv, err)
	}

	meta, err := manager.MetaClient("127.0.0.1:9091")

	if err != nil || meta == nil {
		t.Fatalf("Meta client=%v err=%v", meta, err)
	}

	if manager.Count() != 1 {
		t.Fatal("services did not share a connection")
	}
}

func TestConnectionManagerInvalidServiceEndpoints(t *testing.T) {
	manager := newTestConnectionManager(t)

	kv, err := manager.KVClient("")

	if kv != nil || !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("KV client=%v err=%v", kv, err)
	}

	meta, err := manager.MetaClient("")

	if meta != nil || !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("Meta client=%v err=%v", meta, err)
	}
}

func TestConnectionManagerClose(t *testing.T) {
	manager := newTestConnectionManager(t)

	conn, err := manager.Connection("127.0.0.1:9091")

	if err != nil {
		t.Fatal(err)
	}

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	if manager.Count() != 0 {
		t.Fatal("closed manager retained connections")
	}

	if conn.GetState() != connectivity.Shutdown {
		t.Fatal("connection was not shut down")
	}

	if err := manager.Close(); err != nil {
		t.Fatal("repeated close failed:", err)
	}
}

func TestConnectionManagerRejectsAfterClose(t *testing.T) {
	manager := newTestConnectionManager(t)

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	conn, err := manager.Connection("127.0.0.1:9091")

	if conn != nil ||
		!errors.Is(err, ErrConnectionManagerClosed) {
		t.Fatalf("conn=%v err=%v", conn, err)
	}

	kv, err := manager.KVClient("127.0.0.1:9091")

	if kv != nil ||
		!errors.Is(err, ErrConnectionManagerClosed) {
		t.Fatalf("KV client=%v err=%v", kv, err)
	}

	meta, err := manager.MetaClient("127.0.0.1:9091")

	if meta != nil ||
		!errors.Is(err, ErrConnectionManagerClosed) {
		t.Fatalf("Meta client=%v err=%v", meta, err)
	}
}

func TestConnectionManagerConcurrentReuse(t *testing.T) {
	manager := newTestConnectionManager(t)

	const (
		workers = 32
		lookups = 1000
	)

	first, err := manager.Connection("127.0.0.1:9091")

	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < lookups; j++ {
				conn, err := manager.Connection(
					"127.0.0.1:9091",
				)

				if err != nil || conn != first {
					t.Errorf(
						"connection reuse failed: %v",
						err,
					)
					return
				}
			}
		}()
	}

	wg.Wait()

	if manager.Count() != 1 {
		t.Fatal("concurrent calls created duplicates")
	}

	t.Logf(
		"validated %d concurrent connection lookups",
		workers*lookups,
	)
}

func TestConnectionManagerConcurrentClose(t *testing.T) {
	manager := newTestConnectionManager(t)

	var wg sync.WaitGroup

	start := make(chan struct{})

	for i := 0; i < 32; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			for j := 0; j < 100; j++ {
				_, err := manager.Connection(
					"127.0.0.1:9091",
				)

				if err != nil &&
					!errors.Is(
						err,
						ErrConnectionManagerClosed,
					) {
					t.Error(err)
					return
				}
			}
		}()
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if manager.Count() != 0 {
		t.Fatal("connections remained after close")
	}
}

// Real gRPC test services.

type connectionTestMetaServer struct {
	shardraftpb.UnimplementedMetaServiceServer
}

func (s *connectionTestMetaServer) GetShardMap(
	ctx context.Context,
	request *shardraftpb.GetShardMapRequest,
) (*shardraftpb.GetShardMapResponse, error) {
	return &shardraftpb.GetShardMapResponse{
		ShardMap: testShardMap(),
	}, nil
}

type connectionTestKVServer struct {
	shardraftpb.UnimplementedKVServiceServer
}

func (s *connectionTestKVServer) Get(
	ctx context.Context,
	request *shardraftpb.GetRequest,
) (*shardraftpb.GetResponse, error) {
	return &shardraftpb.GetResponse{
		Key:   request.GetKey(),
		Value: "100",
		Found: true,
	}, nil
}

func startConnectionTestServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	server := grpc.NewServer()

	shardraftpb.RegisterMetaServiceServer(
		server,
		&connectionTestMetaServer{},
	)

	shardraftpb.RegisterKVServiceServer(
		server,
		&connectionTestKVServer{},
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

	return listener.Addr().String()
}

func TestConnectionManagerWireMetadata(t *testing.T) {
	address := startConnectionTestServer(t)
	manager := newTestConnectionManager(t)

	client, err := manager.MetaClient(address)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	response, err := client.GetShardMap(
		ctx,
		&shardraftpb.GetShardMapRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	table, err := NewRoutingTable(response.GetShardMap())

	if err != nil {
		t.Fatal(err)
	}

	if table.Epoch() != 1 ||
		table.ShardCount() != 1 {
		t.Fatal("unexpected metadata response")
	}
}

func TestConnectionManagerWireSharedServices(t *testing.T) {
	address := startConnectionTestServer(t)
	manager := newTestConnectionManager(t)

	meta, err := manager.MetaClient(address)

	if err != nil {
		t.Fatal(err)
	}

	kv, err := manager.KVClient(address)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	metadata, err := meta.GetShardMap(
		ctx,
		&shardraftpb.GetShardMapRequest{},
	)

	if err != nil {
		t.Fatal(err)
	}

	cache := newTestMetadataCache(t)

	if err := cache.Update(
		metadata.GetShardMap(),
	); err != nil {
		t.Fatal(err)
	}

	shard, err := cache.Route("alpha")

	if err != nil {
		t.Fatal(err)
	}

	response, err := kv.Get(
		ctx,
		&shardraftpb.GetRequest{
			Header: &shardraftpb.RequestHeader{
				ShardId: shard.GetShardId(),
				Epoch:   shard.GetEpoch(),
			},
			Key: "alpha",
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if !response.GetFound() ||
		response.GetValue() != "100" {
		t.Fatal("unexpected KV response")
	}

	if manager.Count() != 1 {
		t.Fatal("wire services used separate connections")
	}
}

func TestConnectionManagerWireCanceledContext(t *testing.T) {
	address := startConnectionTestServer(t)
	manager := newTestConnectionManager(t)

	client, err := manager.MetaClient(address)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, err = client.GetShardMap(
		ctx,
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConnectionManagerWireExpiredDeadline(t *testing.T) {
	address := startConnectionTestServer(t)
	manager := newTestConnectionManager(t)

	client, err := manager.MetaClient(address)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	_, err = client.GetShardMap(
		ctx,
		&shardraftpb.GetShardMapRequest{},
	)

	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("unexpected error: %v", err)
	}
}

func BenchmarkConnectionManagerReuse(b *testing.B) {
	manager := newTestConnectionManager(b)

	if _, err := manager.Connection(
		"127.0.0.1:9091",
	); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = manager.Connection("127.0.0.1:9091")
	}
}
