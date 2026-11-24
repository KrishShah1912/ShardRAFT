package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

var errLeaderUnavailable = errors.New("leader unavailable")

type leaderTarget struct {
	ID      raft.NodeID
	Term    uint64
	Address string
	Local   bool
}

type peerStatusResponse struct {
	ID       raft.NodeID `json:"id"`
	Role     raft.Role   `json:"role"`
	Term     uint64      `json:"term"`
	LeaderID raft.NodeID `json:"leader_id"`
}

type leaderDiscovery struct {
	localID raft.NodeID
	peers   map[raft.NodeID]string
	status  func() (raft.Status, error)
	client  *http.Client
}

func newLeaderDiscovery(
	localID raft.NodeID,
	peers []transport.Peer,
	status func() (raft.Status, error),
) *leaderDiscovery {
	addresses := make(map[raft.NodeID]string)

	for _, peer := range peers {
		addresses[peer.ID] = peer.Address
	}

	return &leaderDiscovery{
		localID: localID,
		peers:   addresses,
		status:  status,
		client: &http.Client{
			Timeout: 500 * time.Millisecond,
			CheckRedirect: func(
				req *http.Request,
				via []*http.Request,
			) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (d *leaderDiscovery) queryPeer(
	ctx context.Context,
	id raft.NodeID,
	address string,
) (peerStatusResponse, error) {
	var result peerStatusResponse

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://"+address+"/status",
		nil,
	)
	if err != nil {
		return result, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf(
			"peer %d returned HTTP %d",
			id,
			resp.StatusCode,
		)
	}

	decoder := json.NewDecoder(
		io.LimitReader(resp.Body, 64<<10),
	)

	if err := decoder.Decode(&result); err != nil {
		return result, err
	}

	if result.ID != id {
		return result, fmt.Errorf(
			"unexpected peer ID: got %d, want %d",
			result.ID,
			id,
		)
	}

	return result, nil
}

func (d *leaderDiscovery) discover(
	ctx context.Context,
) (leaderTarget, error) {
	if err := ctx.Err(); err != nil {
		return leaderTarget{}, err
	}

	local, err := d.status()
	if err != nil {
		return leaderTarget{}, err
	}

	results := make(
		chan peerStatusResponse,
		len(d.peers),
	)

	var wg sync.WaitGroup

	for id, address := range d.peers {
		wg.Add(1)

		go func(id raft.NodeID, address string) {
			defer wg.Done()

			status, err := d.queryPeer(
				ctx, id, address,
			)
			if err == nil {
				results <- status
			}
		}(id, address)
	}

	wg.Wait()
	close(results)

	if err := ctx.Err(); err != nil {
		return leaderTarget{}, err
	}

	latest, err := d.status()
	if err != nil {
		return leaderTarget{}, err
	}

	highestTerm := latest.Term
	if local.Term > highestTerm {
		highestTerm = local.Term
	}

	statuses := make([]peerStatusResponse, 0)

	for status := range results {
		statuses = append(statuses, status)

		if status.Term > highestTerm {
			highestTerm = status.Term
		}
	}

	var target leaderTarget

	if latest.Role == raft.Leader &&
		latest.LeaderID == d.localID &&
		latest.Term == highestTerm {
		target = leaderTarget{
			ID:    d.localID,
			Term:  latest.Term,
			Local: true,
		}
	}

	for _, status := range statuses {
		if status.Term != highestTerm {
			continue
		}

		if status.Role != raft.Leader ||
			status.LeaderID != status.ID {
			continue
		}

		if target.ID != 0 &&
			target.ID != status.ID {
			return leaderTarget{}, errLeaderUnavailable
		}

		target = leaderTarget{
			ID:      status.ID,
			Term:    status.Term,
			Address: d.peers[status.ID],
		}
	}

	if target.ID == 0 {
		return leaderTarget{}, errLeaderUnavailable
	}

	return target, nil
}
