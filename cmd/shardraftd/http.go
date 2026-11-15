package main

import (
	"encoding/json"
	"net/http"

	"github.com/KrishShah1912/shardraft/internal/shard"
)

func statusHandler(node *shard.Shard) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		status, err := node.Status()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		applied, err := node.AppliedIndex()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":            status.ID,
			"role":          status.Role,
			"term":          status.Term,
			"leader_id":     status.LeaderID,
			"commit_index":  status.Commit,
			"applied_index": applied,
		})
	})

	registerKVHandlers(mux, node)
	registerKVMutationHandlers(mux, node)
	return mux
}
