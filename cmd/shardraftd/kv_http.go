package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

type kvWriteRequest struct {
	Value     string `json:"value"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func registerKVHandlers(
	mux *http.ServeMux,
	node *shard.Shard,
) {
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		status, err := node.Status()
		if err != nil {
			writeRoutingError(
				w,
				http.StatusServiceUnavailable,
				routingCodeUnavailable,
				err.Error(),
				raft.Status{},
			)
			return
		}

		if status.Role != raft.Leader {
			writeRoutingError(
				w,
				http.StatusServiceUnavailable,
				routingCodeNotLeader,
				"not leader",
				status,
			)
			return
		}

		// Local leader reads are not yet guaranteed linearizable
		// in a multi-node deployment.
		value, found, err := node.Get(key)
		if err != nil {
			writeRoutingError(
				w,
				http.StatusServiceUnavailable,
				routingCodeUnavailable,
				err.Error(),
				raft.Status{},
			)
			return
		}

		if !found {
			writeJSON(w, http.StatusNotFound, map[string]any{
				"error": "key not found",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"key":   key,
			"value": value,
		})
	})

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		defer r.Body.Close()

		var input kvWriteRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid JSON request", http.StatusBadRequest)
			return
		}

		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			http.Error(w, "request must contain one JSON object", http.StatusBadRequest)
			return
		}

		if input.ClientID == "" || input.RequestID == "" {
			http.Error(w, "client_id and request_id are required", http.StatusBadRequest)
			return
		}

		command := kvstore.Command{
			Op:        kvstore.OpPut,
			Key:       key,
			Value:     input.Value,
			ClientID:  input.ClientID,
			RequestID: input.RequestID,
		}

		if err := command.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		submitMutation(w, r, node, command)
	})
}

func writeKVError(
	w http.ResponseWriter,
	node *shard.Shard,
	err error,
) {
	status, _ := node.Status()

	switch {
	case errors.Is(err, kvstore.ErrRequestConflict):
		http.Error(w, err.Error(), http.StatusConflict)

	case errors.Is(err, shard.ErrRequestPending):
		http.Error(w, err.Error(), http.StatusConflict)

	case errors.Is(err, raft.ErrNotLeader):
		writeRoutingError(
			w,
			http.StatusServiceUnavailable,
			routingCodeNotLeader,
			err.Error(),
			status,
		)

	default:
		writeRoutingError(
			w,
			http.StatusServiceUnavailable,
			routingCodeUnavailable,
			err.Error(),
			status,
		)
	}
}

func writeUnknownMutation(
	w http.ResponseWriter,
	node *shard.Shard,
) {
	status, _ := node.Status()

	writeRoutingError(
		w,
		http.StatusGatewayTimeout,
		routingCodeUnknown,
		"commit not confirmed; request outcome unknown",
		status,
	)
}
