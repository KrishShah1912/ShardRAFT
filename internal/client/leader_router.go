package client

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

var (
	ErrNoReplicaEndpoint = errors.New("no usable replica endpoint")
	ErrInvalidLeaderHint = errors.New("invalid leader hint")
	ErrStaleLeaderHint   = errors.New("stale leader hint")
)

// ReplicaEndpoint identifies a reachable replica candidate.
type ReplicaEndpoint struct {
	NodeID  uint64
	Address string
}

type leaderHint struct {
	nodeID uint64
	term   uint64
	epoch  uint64
}

// LeaderRouter maintains advisory leader information.
type LeaderRouter struct {
	mu    sync.RWMutex
	hints map[uint64]leaderHint
}

func NewLeaderRouter() *LeaderRouter {
	return &LeaderRouter{
		hints: make(map[uint64]leaderHint),
	}
}

// Candidates returns usable replica endpoints with the
// preferred leader first and remaining replicas ordered
// by node ID.
func (router *LeaderRouter) Candidates(
	shard *shardraftpb.ShardInfo,
) ([]ReplicaEndpoint, error) {
	if shard == nil || shard.GetShardId() == 0 {
		return nil, ErrInvalidShardMap
	}

	router.mu.RLock()
	hint, exists := router.hints[shard.GetShardId()]
	router.mu.RUnlock()

	preferred := shard.GetLeaderId()

	if exists && hint.epoch == shard.GetEpoch() {
		preferred = hint.nodeID
	}

	candidates := make(
		[]ReplicaEndpoint,
		0,
		len(shard.GetReplicas()),
	)

	for _, replica := range shard.GetReplicas() {
		if replica == nil || replica.GetNodeId() == 0 {
			continue
		}

		address, err := normalizeEndpoint(
			replica.GetAddress(),
		)

		if err != nil {
			continue
		}

		candidates = append(candidates, ReplicaEndpoint{
			NodeID:  replica.GetNodeId(),
			Address: address,
		})
	}

	if len(candidates) == 0 {
		return nil, ErrNoReplicaEndpoint
	}

	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i].NodeID
		right := candidates[j].NodeID

		if left == preferred {
			return right != preferred
		}

		if right == preferred {
			return false
		}

		return left < right
	})

	return candidates, nil
}

// Preferred returns the first usable replica candidate.
func (router *LeaderRouter) Preferred(
	shard *shardraftpb.ShardInfo,
) (ReplicaEndpoint, error) {
	candidates, err := router.Candidates(shard)

	if err != nil {
		return ReplicaEndpoint{}, err
	}

	return candidates[0], nil
}

// ObserveHint accepts a NOT_LEADER response only when
// its leader belongs to the current shard membership.
//
// Addresses from routing errors are never trusted over
// the addresses in validated shard metadata.
func (router *LeaderRouter) ObserveHint(
	shard *shardraftpb.ShardInfo,
	routingError *shardraftpb.RoutingError,
) error {
	if shard == nil ||
		shard.GetShardId() == 0 ||
		shard.GetEpoch() == 0 ||
		routingError == nil ||
		routingError.GetCode() !=
			shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER ||
		routingError.GetShardId() != shard.GetShardId() ||
		routingError.GetEpoch() != shard.GetEpoch() ||
		routingError.GetLeaderId() == 0 ||
		routingError.GetTerm() == 0 {
		return ErrInvalidLeaderHint
	}

	leaderID := routingError.GetLeaderId()

	member := false

	for _, replica := range shard.GetReplicas() {
		if replica.GetNodeId() == leaderID {
			member = true
			break
		}
	}

	if !member {
		return fmt.Errorf(
			"%w: leader %d is not a replica",
			ErrInvalidLeaderHint,
			leaderID,
		)
	}

	router.mu.Lock()
	defer router.mu.Unlock()

	current, exists := router.hints[shard.GetShardId()]

	if exists {
		if shard.GetEpoch() < current.epoch {
			return ErrStaleLeaderHint
		}

		if shard.GetEpoch() == current.epoch {
			if routingError.GetTerm() < current.term {
				return ErrStaleLeaderHint
			}

			if routingError.GetTerm() == current.term &&
				current.nodeID != 0 &&
				current.nodeID != leaderID {
				return ErrInvalidLeaderHint
			}
		}
	}

	router.hints[shard.GetShardId()] = leaderHint{
		nodeID: leaderID,
		term:   routingError.GetTerm(),
		epoch:  shard.GetEpoch(),
	}

	return nil
}

// Invalidate clears the preferred leader while retaining
// the observed epoch and term to reject stale hints.
func (router *LeaderRouter) Invalidate(
	shardID uint64,
	nodeID uint64,
) {
	router.mu.Lock()
	defer router.mu.Unlock()

	hint, exists := router.hints[shardID]

	if exists && hint.nodeID == nodeID {
		hint.nodeID = 0
		router.hints[shardID] = hint
	}
}
