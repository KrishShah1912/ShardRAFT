package main

import (
	"encoding/json"
	"net/http"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

func registerMultiMetadataHTTP(
	mux *http.ServeMux,
	group *meta.Group,
	service *multiMetaGRPCServer,
	nodeID raft.NodeID,
) {
	mux.HandleFunc(
		"GET /api/v1/metadata/status",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")

			if group == nil {
				http.Error(
					w,
					"node does not host metadata group",
					http.StatusNotFound,
				)
				return
			}

			state, applied, err := group.StatusSnapshot()

			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			role := "unknown"

			switch state.Role {
			case raft.Leader:
				role = "leader"
			case raft.Follower:
				role = "follower"
			case raft.Candidate:
				role = "candidate"
			}

			w.Header().Set(
				"Content-Type",
				"application/json",
			)

			_ = json.NewEncoder(w).Encode(
				map[string]any{
					"node_id":       nodeID,
					"group_id":      meta.MetadataGroupID,
					"role":          role,
					"term":          state.Term,
					"leader_id":     state.LeaderID,
					"commit_index":  state.Commit,
					"applied_index": applied,
					"consistency":   "local",
				},
			)
		},
	)

	mux.HandleFunc(
		"GET /api/v1/metadata/shard-map",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")

			if group == nil {
				http.Error(
					w,
					"node does not host metadata group",
					http.StatusNotFound,
				)
				return
			}

			response, err := service.GetShardMap(
				r.Context(),
				&shardraftpb.GetShardMapRequest{},
			)

			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			w.Header().Set(
				"Content-Type",
				"application/json",
			)

			_ = json.NewEncoder(w).Encode(
				map[string]any{
					"node_id":     nodeID,
					"consistency": "local",
					"shard_map":   response.ShardMap,
				},
			)
		},
	)
}
