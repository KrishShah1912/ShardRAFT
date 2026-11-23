package main

import (
	"net/http"

	"github.com/KrishShah1912/shardraft/internal/observability"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

func registerAdminHandlers(
	mux *http.ServeMux,
	node *shard.Shard,
	bus *observability.Bus,
) {
	mux.HandleFunc("GET /api/v1/node", func(
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

		writeJSON(w, http.StatusOK, map[string]any{
			"id":              status.ID,
			"role":            status.Role,
			"term":            status.Term,
			"leader_id":       status.LeaderID,
			"commit_index":    status.Commit,
			"applied_index":   applied,
			"latest_event_id": bus.LatestEventID(),
		})
	})

	registerEventHandlers(mux, bus)
}
