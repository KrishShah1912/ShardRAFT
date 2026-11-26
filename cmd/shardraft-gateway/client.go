package main

import (
	"context"
	"time"

	"github.com/KrishShah1912/shardraft/internal/client"
)

// gatewayKVClient defines the operations required by
// the HTTP gateway without duplicating client routing.
type gatewayKVClient interface {
	Get(
		ctx context.Context,
		key string,
	) (string, bool, error)

	GetLocal(
		ctx context.Context,
		key string,
	) (string, bool, error)

	PutWithIdentity(
		ctx context.Context,
		key string,
		value string,
		identity client.MutationIdentity,
	) (client.MutationResult, error)

	DeleteWithIdentity(
		ctx context.Context,
		key string,
		identity client.MutationIdentity,
	) (client.MutationResult, error)

	CompareAndSwapWithIdentity(
		ctx context.Context,
		key string,
		expected string,
		value string,
		identity client.MutationIdentity,
	) (client.MutationResult, error)
}

// Verify compatibility with the existing gRPC client.
var _ gatewayKVClient = (*client.Client)(nil)

func newGatewayClient(cfg gatewayConfig) (*client.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	clientCfg := client.DefaultConfig(cfg.Endpoints)

	clientCfg.MetadataTTL = 30 * time.Second
	clientCfg.MetadataTimeout = time.Second

	clientCfg.Retry = client.RetryConfig{
		MaxAttempts:    5,
		AttemptTimeout: 2 * time.Second,
		InitialBackoff: 25 * time.Millisecond,
		MaxBackoff:     250 * time.Millisecond,
	}

	return client.NewClient(clientCfg)
}
