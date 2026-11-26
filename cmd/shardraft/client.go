package main

import (
	"context"

	"github.com/KrishShah1912/shardraft/internal/client"
)

type cliKVClient interface {
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

	NewMutationIdentity() (client.MutationIdentity, error)

	Close() error
}

var _ cliKVClient = (*client.Client)(nil)

type cliClientFactory func(cliConfig) (cliKVClient, error)

func newCLIClient(cfg cliConfig) (cliKVClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	clientCfg := client.DefaultConfig(cfg.Endpoints)
	clientCfg.ClientID = cfg.ClientID

	return client.NewClient(clientCfg)
}
