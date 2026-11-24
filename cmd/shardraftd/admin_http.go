package main

import (
	"net/http"
	"sort"

	"github.com/KrishShah1912/shardraft/internal/observability"
	"github.com/KrishShah1912/shardraft/internal/shard"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

type kvDebugItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func registerAdminHandlers(
	mux *http.ServeMux,
	node *shard.Shard,
	bus *observability.Bus,
	sender *transport.HTTPTransport,
) {
	mux.HandleFunc(
		"GET /api/v1/node",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			status, err := node.Status()
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			applied, err := node.AppliedIndex()
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			writeJSON(
				w,
				http.StatusOK,
				map[string]any{
					"id":              status.ID,
					"role":            status.Role,
					"term":            status.Term,
					"leader_id":       status.LeaderID,
					"commit_index":    status.Commit,
					"applied_index":   applied,
					"latest_event_id": bus.LatestEventID(),
				},
			)
		},
	)

	mux.HandleFunc(
		"GET /api/v1/admin/raft",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			state, err := node.DebugRaft()
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			writeJSON(
				w,
				http.StatusOK,
				state,
			)
		},
	)

	mux.HandleFunc(
		"GET /api/v1/admin/raft/log",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			log, err := node.DebugRaftLog()
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			writeJSON(
				w,
				http.StatusOK,
				log,
			)
		},
	)

	mux.HandleFunc(
		"GET /api/v1/admin/kv",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			data, applied, err := node.DebugKV()
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			keys := make(
				[]string,
				0,
				len(data),
			)

			for key := range data {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			items := make(
				[]kvDebugItem,
				0,
				len(keys),
			)

			for _, key := range keys {
				items = append(
					items,
					kvDebugItem{
						Key:   key,
						Value: data[key],
					},
				)
			}

			writeJSON(
				w,
				http.StatusOK,
				map[string]any{
					"count":         len(items),
					"applied_index": applied,
					"items":         items,
				},
			)
		},
	)

	mux.HandleFunc(
		"GET /api/v1/admin/storage",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			state, err := node.DebugStorage()
			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusServiceUnavailable,
				)
				return
			}

			writeJSON(
				w,
				http.StatusOK,
				state,
			)
		},
	)

	mux.HandleFunc(
		"GET /api/v1/admin/transport",
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			if sender == nil {
				writeJSON(
					w,
					http.StatusOK,
					transport.Stats{},
				)
				return
			}

			writeJSON(
				w,
				http.StatusOK,
				sender.Stats(),
			)
		},
	)

	registerEventHandlers(
		mux,
		bus,
	)
}
