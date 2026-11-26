package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"

	"github.com/KrishShah1912/shardraft/internal/meta"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

func registerMultiRaftAdminHTTP(
	mux *http.ServeMux,
	manager *shard.Manager,
	nodeID raft.NodeID,
) {
	register := func(
		path string,
		read func(*shard.Shard) (any, error),
	) {
		mux.HandleFunc(
			"GET "+path,
			func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(
					"Cache-Control",
					"no-store",
				)

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

				if manager == nil {
					http.Error(
						w,
						"shard manager unavailable",
						http.StatusServiceUnavailable,
					)
					return
				}

				group, err := manager.Group(
					meta.ShardID(id),
				)

				if err != nil {
					http.Error(
						w,
						"shard is not hosted on this node",
						http.StatusNotFound,
					)
					return
				}

				result, err := read(group.Shard())

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
						"shard_id":    id,
						"consistency": "local",
						"data":        result,
					},
				)
			},
		)
	}

	register(
		"/api/v1/shards/{id}/raft",
		func(s *shard.Shard) (any, error) {
			return s.DebugRaft()
		},
	)

	register(
		"/api/v1/shards/{id}/raft/log",
		func(s *shard.Shard) (any, error) {
			return s.DebugRaftLog()
		},
	)

	register(
		"/api/v1/shards/{id}/storage",
		func(s *shard.Shard) (any, error) {
			return s.DebugStorage()
		},
	)

	register(
		"/api/v1/shards/{id}/kv",
		func(s *shard.Shard) (any, error) {
			data, applied, err := s.DebugKV()

			if err != nil {
				return nil, err
			}

			keys := make([]string, 0, len(data))

			for key := range data {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			items := make(
				[]map[string]string,
				0,
				len(keys),
			)

			for _, key := range keys {
				items = append(
					items,
					map[string]string{
						"key":   key,
						"value": data[key],
					},
				)
			}

			return map[string]any{
				"count":         len(items),
				"applied_index": applied,
				"items":         items,
			}, nil
		},
	)
}
