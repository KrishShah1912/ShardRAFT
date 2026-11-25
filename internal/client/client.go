package client

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidClientConfig = errors.New("invalid client configuration")
	ErrInvalidKey          = errors.New("invalid key")
	ErrInvalidValue        = errors.New("invalid value")
	ErrInvalidResponse     = errors.New("invalid KV response")
)

// Config controls client discovery, connections and retries.
type Config struct {
	Endpoints       []string
	ClientID        string
	MetadataTTL     time.Duration
	MetadataTimeout time.Duration
	Retry           RetryConfig
	DialOptions     []grpc.DialOption
}

func DefaultConfig(endpoints []string) Config {
	return Config{
		Endpoints:       append([]string(nil), endpoints...),
		MetadataTTL:     30 * time.Second,
		MetadataTimeout: time.Second,
		Retry:           DefaultRetryConfig(),
	}
}

// Client combines metadata discovery, hash-slot routing,
// leader selection, connection reuse and bounded retries.
type Client struct {
	clientID    string
	cache       *MetadataCache
	connections *ConnectionManager
	refresher   *MetadataRefresher
	router      *LeaderRouter
	retry       *RetryExecutor
}

func NewClient(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 ||
		cfg.MetadataTTL <= 0 ||
		cfg.MetadataTimeout <= 0 ||
		!utf8.ValidString(cfg.ClientID) {
		return nil, ErrInvalidClientConfig
	}

	executor, err := NewRetryExecutor(cfg.Retry)
	if err != nil {
		return nil, err
	}

	clientID := cfg.ClientID

	if clientID == "" {
		clientID, err = NewRequestID()
		if err != nil {
			return nil, err
		}
	}

	cache, err := NewMetadataCache(cfg.MetadataTTL)
	if err != nil {
		return nil, err
	}

	connections := NewConnectionManager(cfg.DialOptions...)

	refresher, err := NewGRPCMetadataRefresher(
		cache,
		connections,
		cfg.Endpoints,
		cfg.MetadataTimeout,
	)

	if err != nil {
		_ = connections.Close()
		return nil, err
	}

	return &Client{
		clientID:    clientID,
		cache:       cache,
		connections: connections,
		refresher:   refresher,
		router:      NewLeaderRouter(),
		retry:       executor,
	}, nil
}

func (c *Client) ClientID() string {
	return c.clientID
}

func (c *Client) Close() error {
	return c.connections.Close()
}

func (c *Client) RefreshMetadata(ctx context.Context) error {
	return c.refresher.Refresh(ctx)
}

func (c *Client) NewMutationIdentity() (MutationIdentity, error) {
	return NewMutationIdentity(c.clientID)
}

func validateClientKey(key string) error {
	if key == "" || !utf8.ValidString(key) {
		return ErrInvalidKey
	}

	return nil
}

func validateClientValue(value string) error {
	if !utf8.ValidString(value) {
		return ErrInvalidValue
	}

	return nil
}

func validateClientIdentity(identity MutationIdentity) error {
	if err := identity.Validate(); err != nil {
		return err
	}

	if !utf8.ValidString(identity.ClientID) ||
		!utf8.ValidString(identity.RequestID) {
		return ErrInvalidMutationIdentity
	}

	return nil
}

type routedOperation func(
	context.Context,
	shardraftpb.KVServiceClient,
	*shardraftpb.RequestHeader,
) (*shardraftpb.RoutingError, error)

// unavailableBeforeDispatch represents a failure before a
// KV operation has been sent. Such failures are retryable
// but do not themselves create an ambiguous mutation.
func unavailableBeforeDispatch(err error) *shardraftpb.RoutingError {
	return &shardraftpb.RoutingError{
		Code:    shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
		Message: err.Error(),
	}
}

func selectReplica(
	candidates []ReplicaEndpoint,
	attempted map[string]bool,
) ReplicaEndpoint {
	for _, candidate := range candidates {
		if !attempted[candidate.Address] {
			return candidate
		}
	}

	clear(attempted)

	return candidates[0]
}

func (c *Client) execute(
	ctx context.Context,
	key string,
	mutation bool,
	identity MutationIdentity,
	operation routedOperation,
) error {
	if err := validateClientKey(key); err != nil {
		return err
	}

	if mutation {
		if err := validateClientIdentity(identity); err != nil {
			return err
		}
	}

	// Attempt tracking belongs to this operation only.
	// Concurrent operations do not share this state.
	attempted := make(map[string]bool)

	var previousTable *RoutingTable

	return c.retry.Execute(
		ctx,
		mutation,
		identity,
		func(
			attemptCtx context.Context,
			_ int,
			stableIdentity MutationIdentity,
		) (*shardraftpb.RoutingError, error) {

			if err := c.refresher.Ensure(attemptCtx); err != nil {
				return unavailableBeforeDispatch(err), nil
			}

			table, err := c.cache.Snapshot()
			if err != nil {
				return unavailableBeforeDispatch(err), nil
			}

			shard, err := table.Route(key)
			if err != nil {
				return nil, err
			}

			if previousTable != table {
				clear(attempted)
				previousTable = table
			}

			candidates, err := c.router.Candidates(shard)
			if err != nil {
				c.cache.Invalidate()
				return unavailableBeforeDispatch(err), nil
			}

			target := selectReplica(candidates, attempted)

			attempted[target.Address] = true

			kv, err := c.connections.KVClient(target.Address)
			if err != nil {
				if errors.Is(err, ErrConnectionManagerClosed) {
					return nil, err
				}

				return unavailableBeforeDispatch(err), nil
			}

			header := &shardraftpb.RequestHeader{
				ShardId: shard.GetShardId(),
				Epoch:   shard.GetEpoch(),
			}

			if mutation {
				header = stableIdentity.Header(
					shard.GetShardId(),
					shard.GetEpoch(),
				)
			}

			if err := attemptCtx.Err(); err != nil {
				return unavailableBeforeDispatch(err), nil
			}

			failure, rpcErr := operation(
				attemptCtx,
				kv,
				header,
			)

			if rpcErr != nil {
				c.router.Invalidate(
					shard.GetShardId(),
					target.NodeID,
				)

				return nil, rpcErr
			}

			if failure == nil {
				return nil, nil
			}

			switch failure.GetCode() {
			case shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER:

				c.router.Invalidate(
					shard.GetShardId(),
					target.NodeID,
				)

				// Only validated metadata addresses are used.
				_ = c.router.ObserveHint(shard, failure)

			case shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD:

				err := c.refresher.HandleWrongShard(
					attemptCtx,
					table,
					failure,
				)

				if err != nil {
					c.cache.Invalidate()
				}

			case shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
				shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN:

				c.router.Invalidate(
					shard.GetShardId(),
					target.NodeID,
				)
			}

			return failure, nil
		},
	)
}

// Get performs a linearizable read by default.
func (c *Client) Get(
	ctx context.Context,
	key string,
) (string, bool, error) {
	return c.get(
		ctx,
		key,
		shardraftpb.ReadMode_READ_MODE_LINEARIZABLE,
	)
}

// GetLocal explicitly permits a potentially stale local read.
func (c *Client) GetLocal(
	ctx context.Context,
	key string,
) (string, bool, error) {
	return c.get(
		ctx,
		key,
		shardraftpb.ReadMode_READ_MODE_LOCAL,
	)
}

func (c *Client) get(
	ctx context.Context,
	key string,
	mode shardraftpb.ReadMode,
) (string, bool, error) {
	var value string
	var found bool

	err := c.execute(
		ctx,
		key,
		false,
		MutationIdentity{},
		func(
			attemptCtx context.Context,
			kv shardraftpb.KVServiceClient,
			header *shardraftpb.RequestHeader,
		) (*shardraftpb.RoutingError, error) {

			response, err := kv.Get(
				attemptCtx,
				&shardraftpb.GetRequest{
					Header:   header,
					Key:      key,
					ReadMode: mode,
				},
			)

			if err != nil {
				return nil, err
			}

			if response == nil {
				return nil, ErrInvalidResponse
			}

			failure := response.GetError()

			if failure != nil {
				switch failure.GetCode() {
				case shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND:

					if response.GetFound() {
						return nil, ErrInvalidResponse
					}

					value = ""
					found = false
					return nil, nil

				case shardraftpb.ErrorCode_ERROR_CODE_OK:
					// Explicit success.

				default:
					return failure, nil
				}
			}

			if response.GetKey() != key {
				return nil, fmt.Errorf(
					"%w: unexpected response key",
					ErrInvalidResponse,
				)
			}

			value = response.GetValue()
			found = response.GetFound()

			return nil, nil
		},
	)

	if err != nil {
		return "", false, err
	}

	return value, found, nil
}

// MutationResult preserves both the server result and the
// identity needed to safely retry an uncertain operation.
type MutationResult struct {
	Applied  bool
	Value    string
	Identity MutationIdentity
}

type mutationRPC func(
	context.Context,
	shardraftpb.KVServiceClient,
	*shardraftpb.RequestHeader,
) (*shardraftpb.MutationResponse, error)

func (c *Client) mutate(
	ctx context.Context,
	key string,
	identity MutationIdentity,
	rpc mutationRPC,
) (MutationResult, error) {
	result := MutationResult{
		Identity: identity,
	}

	err := c.execute(
		ctx,
		key,
		true,
		identity,
		func(
			attemptCtx context.Context,
			kv shardraftpb.KVServiceClient,
			header *shardraftpb.RequestHeader,
		) (*shardraftpb.RoutingError, error) {

			response, err := rpc(attemptCtx, kv, header)

			if err != nil {
				return nil, err
			}

			if response == nil {
				// The operation may already have executed.
				return nil, status.Error(
					codes.Internal,
					ErrInvalidResponse.Error(),
				)
			}

			failure := response.GetError()

			if failure != nil &&
				failure.GetCode() !=
					shardraftpb.ErrorCode_ERROR_CODE_OK {
				return failure, nil
			}

			result.Applied = response.GetApplied()
			result.Value = response.GetValue()

			return nil, nil
		},
	)

	return result, err
}

func (c *Client) Put(
	ctx context.Context,
	key string,
	value string,
) (MutationResult, error) {
	identity, err := c.NewMutationIdentity()
	if err != nil {
		return MutationResult{}, err
	}

	return c.PutWithIdentity(ctx, key, value, identity)
}

func (c *Client) PutWithIdentity(
	ctx context.Context,
	key string,
	value string,
	identity MutationIdentity,
) (MutationResult, error) {
	if err := validateClientValue(value); err != nil {
		return MutationResult{Identity: identity}, err
	}

	return c.mutate(
		ctx,
		key,
		identity,
		func(
			attemptCtx context.Context,
			kv shardraftpb.KVServiceClient,
			header *shardraftpb.RequestHeader,
		) (*shardraftpb.MutationResponse, error) {

			return kv.Put(
				attemptCtx,
				&shardraftpb.PutRequest{
					Header: header,
					Key:    key,
					Value:  value,
				},
			)
		},
	)
}

func (c *Client) Delete(
	ctx context.Context,
	key string,
) (MutationResult, error) {
	identity, err := c.NewMutationIdentity()
	if err != nil {
		return MutationResult{}, err
	}

	return c.DeleteWithIdentity(ctx, key, identity)
}

func (c *Client) DeleteWithIdentity(
	ctx context.Context,
	key string,
	identity MutationIdentity,
) (MutationResult, error) {
	return c.mutate(
		ctx,
		key,
		identity,
		func(
			attemptCtx context.Context,
			kv shardraftpb.KVServiceClient,
			header *shardraftpb.RequestHeader,
		) (*shardraftpb.MutationResponse, error) {

			return kv.Delete(
				attemptCtx,
				&shardraftpb.DeleteRequest{
					Header: header,
					Key:    key,
				},
			)
		},
	)
}

func (c *Client) CompareAndSwap(
	ctx context.Context,
	key string,
	expected string,
	value string,
) (MutationResult, error) {
	identity, err := c.NewMutationIdentity()
	if err != nil {
		return MutationResult{}, err
	}

	return c.CompareAndSwapWithIdentity(
		ctx,
		key,
		expected,
		value,
		identity,
	)
}

func (c *Client) CompareAndSwapWithIdentity(
	ctx context.Context,
	key string,
	expected string,
	value string,
	identity MutationIdentity,
) (MutationResult, error) {
	if err := validateClientValue(expected); err != nil {
		return MutationResult{Identity: identity}, err
	}

	if err := validateClientValue(value); err != nil {
		return MutationResult{Identity: identity}, err
	}

	return c.mutate(
		ctx,
		key,
		identity,
		func(
			attemptCtx context.Context,
			kv shardraftpb.KVServiceClient,
			header *shardraftpb.RequestHeader,
		) (*shardraftpb.MutationResponse, error) {

			return kv.CompareAndSwap(
				attemptCtx,
				&shardraftpb.CompareAndSwapRequest{
					Header:   header,
					Key:      key,
					Expected: expected,
					Value:    value,
				},
			)
		},
	)
}
