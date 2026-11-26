package main

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func registerMultiTransportHTTP(
	mux *http.ServeMux,
	sender *multiRaftTransport,
	nodeID raft.NodeID,
) {
	mux.HandleFunc(
		"GET /api/v1/shards/{id}/transport",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")

			id, err := strconv.ParseUint(
				r.PathValue("id"),
				10,
				64,
			)

			if err != nil || id < 1 || id > 4 {
				http.Error(
					w,
					"invalid data shard ID",
					http.StatusBadRequest,
				)
				return
			}

			if sender == nil {
				http.Error(
					w,
					"transport unavailable",
					http.StatusServiceUnavailable,
				)
				return
			}

			sender.mu.RLock()

			closed := sender.closed
			group, exists := sender.groups[meta.ShardID(id)]

			sender.mu.RUnlock()

			if closed {
				http.Error(
					w,
					"transport closed",
					http.StatusServiceUnavailable,
				)
				return
			}

			if !exists || group == nil {
				http.Error(
					w,
					"shard transport not hosted on this node",
					http.StatusNotFound,
				)
				return
			}

			w.Header().Set(
				"Content-Type",
				"application/json",
			)

			_ = json.NewEncoder(w).Encode(
				map[string]any{
					"node_id":  nodeID,
					"shard_id": id,
					"protocol": "grpc",
					"data":     group.Stats(),
				},
			)
		},
	)
}
