package transport

import (
	"sort"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type PeerStats struct {
	PeerID           raft.NodeID `json:"peer_id"`
	Address          string      `json:"address"`
	QueueDepth       int         `json:"queue_depth"`
	QueueCapacity    int         `json:"queue_capacity"`
	Enqueued         uint64      `json:"enqueued"`
	Sent             uint64      `json:"sent"`
	Delivered        uint64      `json:"delivered"`
	Retries          uint64      `json:"retries"`
	Failures         uint64      `json:"failures"`
	Backpressure     uint64      `json:"backpressure"`
	LastLatencyMS    float64     `json:"last_latency_ms"`
	AverageLatencyMS float64     `json:"average_latency_ms"`
	LastSuccessAt    *time.Time  `json:"last_success_at,omitempty"`
	LastFailureAt    *time.Time  `json:"last_failure_at,omitempty"`
	LastError        string      `json:"last_error,omitempty"`
}

type Stats struct {
	Closed bool        `json:"closed"`
	Peers  []PeerStats `json:"peers"`

	Enqueued     uint64 `json:"enqueued"`
	Sent         uint64 `json:"sent"`
	Delivered    uint64 `json:"delivered"`
	Retries      uint64 `json:"retries"`
	Failures     uint64 `json:"failures"`
	Backpressure uint64 `json:"backpressure"`
}

func (t *HTTPTransport) Stats() Stats {
	t.mu.Lock()

	closed := t.closed
	workers := make(
		[]*peerWorker,
		0,
		len(t.peers),
	)

	for _, worker := range t.peers {
		workers = append(
			workers,
			worker,
		)
	}

	t.mu.Unlock()

	result := Stats{
		Closed: closed,
		Peers:  make([]PeerStats, 0, len(workers)),
	}

	for _, worker := range workers {
		worker.mu.RLock()

		peer := PeerStats{
			PeerID:        worker.id,
			Address:       worker.address,
			QueueDepth:    len(worker.queue),
			QueueCapacity: cap(worker.queue),
			Enqueued:      worker.enqueued,
			Sent:          worker.sent,
			Delivered:     worker.delivered,
			Retries:       worker.retries,
			Failures:      worker.failures,
			Backpressure:  worker.backpressure,
			LastLatencyMS: float64(
				worker.lastLatency.Microseconds(),
			) / 1000,
			LastError: worker.lastError,
		}

		if worker.delivered > 0 {
			peer.AverageLatencyMS =
				float64(
					worker.totalLatency.Microseconds(),
				) /
					1000 /
					float64(worker.delivered)
		}

		if !worker.lastSuccessAt.IsZero() {
			value := worker.lastSuccessAt
			peer.LastSuccessAt = &value
		}

		if !worker.lastFailureAt.IsZero() {
			value := worker.lastFailureAt
			peer.LastFailureAt = &value
		}

		worker.mu.RUnlock()

		result.Enqueued += peer.Enqueued
		result.Sent += peer.Sent
		result.Delivered += peer.Delivered
		result.Retries += peer.Retries
		result.Failures += peer.Failures
		result.Backpressure += peer.Backpressure

		result.Peers = append(
			result.Peers,
			peer,
		)
	}

	sort.Slice(
		result.Peers,
		func(i, j int) bool {
			return result.Peers[i].PeerID <
				result.Peers[j].PeerID
		},
	)

	return result
}
