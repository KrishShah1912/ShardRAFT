package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unifiedTestServer struct {
	pb.UnimplementedMetaServiceServer
	pb.UnimplementedKVServiceServer

	metadata func() *pb.ShardMap

	get func(context.Context, *pb.GetRequest) (*pb.GetResponse, error)
	put func(context.Context, *pb.PutRequest) (*pb.MutationResponse, error)
	del func(context.Context, *pb.DeleteRequest) (*pb.MutationResponse, error)
	cas func(context.Context, *pb.CompareAndSwapRequest) (*pb.MutationResponse, error)

	metaCalls atomic.Int64
}

func (s *unifiedTestServer) GetShardMap(
	ctx context.Context,
	req *pb.GetShardMapRequest,
) (*pb.GetShardMapResponse, error) {
	s.metaCalls.Add(1)

	return &pb.GetShardMapResponse{
		ShardMap: s.metadata(),
	}, nil
}

func (s *unifiedTestServer) Get(
	ctx context.Context,
	req *pb.GetRequest,
) (*pb.GetResponse, error) {
	if s.get == nil {
		return nil, status.Error(codes.Unimplemented, "get")
	}

	return s.get(ctx, req)
}

func (s *unifiedTestServer) Put(
	ctx context.Context,
	req *pb.PutRequest,
) (*pb.MutationResponse, error) {
	if s.put == nil {
		return nil, status.Error(codes.Unimplemented, "put")
	}

	return s.put(ctx, req)
}

func (s *unifiedTestServer) Delete(
	ctx context.Context,
	req *pb.DeleteRequest,
) (*pb.MutationResponse, error) {
	if s.del == nil {
		return nil, status.Error(codes.Unimplemented, "delete")
	}

	return s.del(ctx, req)
}

func (s *unifiedTestServer) CompareAndSwap(
	ctx context.Context,
	req *pb.CompareAndSwapRequest,
) (*pb.MutationResponse, error) {
	if s.cas == nil {
		return nil, status.Error(codes.Unimplemented, "cas")
	}

	return s.cas(ctx, req)
}

func unifiedMap(
	addresses []string,
	leader uint64,
	epoch uint64,
) *pb.ShardMap {
	metadata := testShardMap()

	metadata.Epoch = epoch

	shard := metadata.Shards[0]
	shard.Epoch = epoch
	shard.LeaderId = leader
	shard.Replicas = nil

	for i, address := range addresses {
		shard.Replicas = append(
			shard.Replicas,
			&pb.ReplicaInfo{
				NodeId:  uint64(i + 1),
				Address: address,
			},
		)
	}

	return metadata
}

func startUnifiedServer(
	t *testing.T,
	s *unifiedTestServer,
) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address := listener.Addr().String()

	if s.metadata == nil {
		s.metadata = func() *pb.ShardMap {
			return unifiedMap([]string{address}, 1, 1)
		}
	}

	server := grpc.NewServer()

	pb.RegisterMetaServiceServer(server, s)
	pb.RegisterKVServiceServer(server, s)

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

	return address
}

func newUnifiedClient(
	t *testing.T,
	addresses ...string,
) *Client {
	t.Helper()

	cfg := DefaultConfig(addresses)
	cfg.ClientID = "unified-test-client"
	cfg.Retry.MaxAttempts = 4
	cfg.Retry.InitialBackoff = 0
	cfg.Retry.MaxBackoff = 0

	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})

	return c
}

func unifiedContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	t.Cleanup(cancel)

	return ctx
}

func TestUnifiedClientConfiguration(t *testing.T) {
	cfg := DefaultConfig(nil)

	if _, err := NewClient(cfg); err == nil {
		t.Fatal("expected missing endpoint rejection")
	}

	cfg = DefaultConfig([]string{"127.0.0.1:9091"})

	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if c.ClientID() == "" {
		t.Fatal("expected generated client ID")
	}

	if c.connections.Count() != 0 {
		t.Fatal("connections should be lazy")
	}
}

func TestUnifiedClientInvalidInput(t *testing.T) {
	c := newUnifiedClient(t, "127.0.0.1:9091")
	ctx := unifiedContext(t)

	if _, _, err := c.Get(ctx, ""); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("unexpected key error: %v", err)
	}

	if _, err := c.Put(ctx, "key", string([]byte{255})); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("unexpected value error: %v", err)
	}

	if _, err := c.DeleteWithIdentity(
		ctx, "key", MutationIdentity{},
	); !errors.Is(err, ErrInvalidMutationIdentity) {
		t.Fatalf("unexpected identity error: %v", err)
	}

	if c.connections.Count() != 0 {
		t.Fatal("invalid requests must not create connections")
	}
}

func TestUnifiedClientReadModes(t *testing.T) {
	var linear atomic.Int64
	var local atomic.Int64

	s := &unifiedTestServer{
		get: func(
			ctx context.Context,
			req *pb.GetRequest,
		) (*pb.GetResponse, error) {
			if req.GetHeader().GetShardId() != 1 ||
				req.GetHeader().GetEpoch() != 1 {
				return nil, status.Error(codes.InvalidArgument, "routing header")
			}

			switch req.GetReadMode() {
			case pb.ReadMode_READ_MODE_LINEARIZABLE:
				linear.Add(1)
			case pb.ReadMode_READ_MODE_LOCAL:
				local.Add(1)
			}

			return &pb.GetResponse{
				Key:   req.GetKey(),
				Value: "100",
				Found: true,
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))
	ctx := unifiedContext(t)

	value, found, err := c.Get(ctx, "alpha")
	if err != nil || !found || value != "100" {
		t.Fatalf("get: %q %v %v", value, found, err)
	}

	value, found, err = c.GetLocal(ctx, "alpha")
	if err != nil || !found || value != "100" {
		t.Fatalf("local get: %q %v %v", value, found, err)
	}

	if linear.Load() != 1 || local.Load() != 1 {
		t.Fatal("incorrect read modes")
	}

	if s.metaCalls.Load() != 1 {
		t.Fatal("metadata should be reused")
	}
}

func TestUnifiedClientMissingKey(t *testing.T) {
	s := &unifiedTestServer{
		get: func(
			ctx context.Context,
			req *pb.GetRequest,
		) (*pb.GetResponse, error) {
			return &pb.GetResponse{
				Key: req.GetKey(),
				Error: &pb.RoutingError{
					Code: pb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND,
				},
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))

	value, found, err := c.Get(unifiedContext(t), "missing")

	if err != nil || found || value != "" {
		t.Fatalf("missing key: %q %v %v", value, found, err)
	}
}

func TestUnifiedClientCRUD(t *testing.T) {
	var mu sync.Mutex
	values := make(map[string]string)

	s := &unifiedTestServer{
		get: func(
			ctx context.Context,
			req *pb.GetRequest,
		) (*pb.GetResponse, error) {
			mu.Lock()
			defer mu.Unlock()

			value, found := values[req.GetKey()]

			return &pb.GetResponse{
				Key:   req.GetKey(),
				Value: value,
				Found: found,
			}, nil
		},

		put: func(
			ctx context.Context,
			req *pb.PutRequest,
		) (*pb.MutationResponse, error) {
			mu.Lock()
			defer mu.Unlock()

			if req.GetHeader().GetClientId() == "" ||
				req.GetHeader().GetRequestId() == "" {
				return nil, status.Error(codes.InvalidArgument, "identity")
			}

			values[req.GetKey()] = req.GetValue()

			return &pb.MutationResponse{
				Applied: true,
				Value:   req.GetValue(),
			}, nil
		},

		del: func(
			ctx context.Context,
			req *pb.DeleteRequest,
		) (*pb.MutationResponse, error) {
			mu.Lock()
			defer mu.Unlock()

			value, found := values[req.GetKey()]
			delete(values, req.GetKey())

			return &pb.MutationResponse{
				Applied: found,
				Value:   value,
			}, nil
		},

		cas: func(
			ctx context.Context,
			req *pb.CompareAndSwapRequest,
		) (*pb.MutationResponse, error) {
			mu.Lock()
			defer mu.Unlock()

			current, found := values[req.GetKey()]

			if !found || current != req.GetExpected() {
				return &pb.MutationResponse{
					Applied: false,
					Value:   current,
				}, nil
			}

			values[req.GetKey()] = req.GetValue()

			return &pb.MutationResponse{
				Applied: true,
				Value:   req.GetValue(),
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))
	ctx := unifiedContext(t)

	put, err := c.Put(ctx, "alpha", "100")
	if err != nil || !put.Applied || put.Identity.RequestID == "" {
		t.Fatalf("put: %+v %v", put, err)
	}

	value, found, err := c.Get(ctx, "alpha")
	if err != nil || !found || value != "100" {
		t.Fatalf("get: %q %v %v", value, found, err)
	}

	cas, err := c.CompareAndSwap(ctx, "alpha", "100", "200")
	if err != nil || !cas.Applied {
		t.Fatalf("cas: %+v %v", cas, err)
	}

	mismatch, err := c.CompareAndSwap(ctx, "alpha", "100", "300")
	if err != nil || mismatch.Applied {
		t.Fatalf("cas mismatch: %+v %v", mismatch, err)
	}

	deleted, err := c.Delete(ctx, "alpha")
	if err != nil || !deleted.Applied {
		t.Fatalf("delete: %+v %v", deleted, err)
	}

	_, found, err = c.Get(ctx, "alpha")
	if err != nil || found {
		t.Fatalf("deleted key: %v %v", found, err)
	}

	if put.Identity.RequestID == cas.Identity.RequestID ||
		cas.Identity.RequestID == deleted.Identity.RequestID {
		t.Fatal("different mutations reused an identity")
	}
}

func TestUnifiedClientStableRetryIdentity(t *testing.T) {
	var mu sync.Mutex
	var identities []MutationIdentity

	s := &unifiedTestServer{
		put: func(
			ctx context.Context,
			req *pb.PutRequest,
		) (*pb.MutationResponse, error) {
			mu.Lock()
			defer mu.Unlock()

			identities = append(identities, MutationIdentity{
				ClientID:  req.GetHeader().GetClientId(),
				RequestID: req.GetHeader().GetRequestId(),
			})

			if req.GetKey() != "alpha" || req.GetValue() != "100" {
				return nil, status.Error(codes.InvalidArgument, "payload changed")
			}

			if len(identities) == 1 {
				return nil, status.Error(codes.Unavailable, "response lost")
			}

			return &pb.MutationResponse{
				Applied: true,
				Value:   "100",
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))

	result, err := c.Put(unifiedContext(t), "alpha", "100")
	if err != nil || !result.Applied {
		t.Fatalf("put: %+v %v", result, err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(identities) != 2 ||
		identities[0] != identities[1] ||
		identities[0] != result.Identity {
		t.Fatalf("unstable identities: %+v", identities)
	}
}

func TestUnifiedClientWrongShardRefresh(t *testing.T) {
	var epoch atomic.Uint64
	epoch.Store(1)

	var address string

	s := &unifiedTestServer{
		metadata: func() *pb.ShardMap {
			return unifiedMap(
				[]string{address},
				1,
				epoch.Load(),
			)
		},

		get: func(
			ctx context.Context,
			req *pb.GetRequest,
		) (*pb.GetResponse, error) {
			if req.GetHeader().GetEpoch() == 1 {
				epoch.Store(2)

				return &pb.GetResponse{
					Error: &pb.RoutingError{
						Code:    pb.ErrorCode_ERROR_CODE_WRONG_SHARD,
						ShardId: 1,
						Epoch:   2,
					},
				}, nil
			}

			if req.GetHeader().GetEpoch() != 2 {
				return nil, status.Error(codes.InvalidArgument, "epoch")
			}

			return &pb.GetResponse{
				Key:   req.GetKey(),
				Value: "200",
				Found: true,
			}, nil
		},
	}

	address = startUnifiedServer(t, s)
	c := newUnifiedClient(t, address)

	value, found, err := c.Get(unifiedContext(t), "alpha")

	if err != nil || !found || value != "200" {
		t.Fatalf("get: %q %v %v", value, found, err)
	}

	if s.metaCalls.Load() != 2 || c.cache.Epoch() != 2 {
		t.Fatal("metadata refresh did not complete")
	}
}

func TestUnifiedClientLeaderRedirect(t *testing.T) {
	var followerCalls atomic.Int64
	var leaderCalls atomic.Int64
	var addresses []string

	follower := &unifiedTestServer{
		metadata: func() *pb.ShardMap {
			return unifiedMap(addresses, 1, 1)
		},

		get: func(
			ctx context.Context,
			req *pb.GetRequest,
		) (*pb.GetResponse, error) {
			followerCalls.Add(1)

			return &pb.GetResponse{
				Error: &pb.RoutingError{
					Code:          pb.ErrorCode_ERROR_CODE_NOT_LEADER,
					ShardId:       1,
					Epoch:         1,
					Term:          2,
					LeaderId:      2,
					LeaderAddress: "untrusted.example:9999",
				},
			}, nil
		},
	}

	leader := &unifiedTestServer{
		get: func(
			ctx context.Context,
			req *pb.GetRequest,
		) (*pb.GetResponse, error) {
			leaderCalls.Add(1)

			return &pb.GetResponse{
				Key:   req.GetKey(),
				Value: "leader-value",
				Found: true,
			}, nil
		},
	}

	first := startUnifiedServer(t, follower)
	second := startUnifiedServer(t, leader)

	addresses = []string{first, second}

	c := newUnifiedClient(t, first)
	ctx := unifiedContext(t)

	for i := 0; i < 2; i++ {
		value, found, err := c.Get(ctx, "alpha")

		if err != nil || !found || value != "leader-value" {
			t.Fatalf("redirect: %q %v %v", value, found, err)
		}
	}

	if followerCalls.Load() != 1 || leaderCalls.Load() != 2 {
		t.Fatal("leader hint was not reused")
	}
}

func TestUnifiedClientUnknownOutcome(t *testing.T) {
	var calls atomic.Int64

	s := &unifiedTestServer{
		put: func(
			ctx context.Context,
			req *pb.PutRequest,
		) (*pb.MutationResponse, error) {
			calls.Add(1)

			return &pb.MutationResponse{
				Error: &pb.RoutingError{
					Code: pb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
				},
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))

	result, err := c.Put(unifiedContext(t), "alpha", "100")

	if !errors.Is(err, ErrMutationOutcomeUnknown) ||
		!errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("unexpected outcome: %v", err)
	}

	if result.Identity.RequestID == "" || calls.Load() != 4 {
		t.Fatalf("result=%+v calls=%d", result, calls.Load())
	}
}

func TestUnifiedClientExplicitReplay(t *testing.T) {
	var mu sync.Mutex
	var original MutationIdentity
	var calls int

	s := &unifiedTestServer{
		put: func(
			ctx context.Context,
			req *pb.PutRequest,
		) (*pb.MutationResponse, error) {
			mu.Lock()
			defer mu.Unlock()

			calls++

			identity := MutationIdentity{
				ClientID:  req.GetHeader().GetClientId(),
				RequestID: req.GetHeader().GetRequestId(),
			}

			if calls == 1 {
				original = identity
			}

			if identity != original {
				return nil, status.Error(codes.InvalidArgument, "identity changed")
			}

			if calls <= 4 {
				return &pb.MutationResponse{
					Error: &pb.RoutingError{
						Code: pb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
					},
				}, nil
			}

			return &pb.MutationResponse{
				Applied: true,
				Value:   req.GetValue(),
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))
	ctx := unifiedContext(t)

	first, err := c.Put(ctx, "alpha", "100")
	if !errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("expected uncertain outcome: %v", err)
	}

	second, err := c.PutWithIdentity(
		ctx,
		"alpha",
		"100",
		first.Identity,
	)

	if err != nil || !second.Applied ||
		second.Identity != first.Identity {
		t.Fatalf("replay: %+v %v", second, err)
	}
}

func TestUnifiedClientPermanentRejection(t *testing.T) {
	var calls atomic.Int64

	s := &unifiedTestServer{
		put: func(
			ctx context.Context,
			req *pb.PutRequest,
		) (*pb.MutationResponse, error) {
			calls.Add(1)

			return &pb.MutationResponse{
				Error: &pb.RoutingError{
					Code: pb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
				},
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))

	_, err := c.Put(unifiedContext(t), "alpha", "100")

	var failure *RoutingFailure

	if !errors.As(err, &failure) ||
		failure.Detail.GetCode() != pb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT {
		t.Fatalf("unexpected rejection: %v", err)
	}

	if calls.Load() != 1 {
		t.Fatal("permanent rejection was retried")
	}
}

func TestUnifiedClientCanceledContext(t *testing.T) {
	c := newUnifiedClient(t, "127.0.0.1:9091")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Put(ctx, "alpha", "100")

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancellation: %v", err)
	}

	if errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatal("undispatched mutation marked ambiguous")
	}

	if c.connections.Count() != 0 {
		t.Fatal("canceled request created a connection")
	}
}

func TestUnifiedClientConcurrentOperations(t *testing.T) {
	const workers = 32
	const operations = 20

	var calls atomic.Int64
	var mu sync.Mutex

	seen := make(map[MutationIdentity]bool)

	s := &unifiedTestServer{
		put: func(
			ctx context.Context,
			req *pb.PutRequest,
		) (*pb.MutationResponse, error) {
			identity := MutationIdentity{
				ClientID:  req.GetHeader().GetClientId(),
				RequestID: req.GetHeader().GetRequestId(),
			}

			mu.Lock()

			duplicate := seen[identity]
			seen[identity] = true

			mu.Unlock()

			if duplicate {
				return nil, status.Error(codes.InvalidArgument, "duplicate identity")
			}

			calls.Add(1)

			return &pb.MutationResponse{
				Applied: true,
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < operations; j++ {
				result, err := c.Put(ctx, "alpha", "100")

				if err != nil || !result.Applied {
					t.Errorf("concurrent put: %+v %v", result, err)
					return
				}
			}
		}()
	}

	wg.Wait()

	if calls.Load() != workers*operations {
		t.Fatalf("unexpected operation count: %d", calls.Load())
	}

	if s.metaCalls.Load() != 1 {
		t.Fatalf("metadata calls: %d", s.metaCalls.Load())
	}

	t.Logf("validated %d concurrent mutation identities", calls.Load())
}

func TestUnifiedClientClose(t *testing.T) {
	s := &unifiedTestServer{
		get: func(
			ctx context.Context,
			req *pb.GetRequest,
		) (*pb.GetResponse, error) {
			return &pb.GetResponse{
				Key:   req.GetKey(),
				Found: true,
			}, nil
		},
	}

	c := newUnifiedClient(t, startUnifiedServer(t, s))
	ctx := unifiedContext(t)

	if _, _, err := c.Get(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := c.Get(ctx, "alpha"); !errors.Is(err, ErrConnectionManagerClosed) {
		t.Fatalf("unexpected closed client error: %v", err)
	}
}
