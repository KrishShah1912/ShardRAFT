package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

type kvMutationRequest struct {
	Value     string `json:"value,omitempty"`
	Expected  string `json:"expected,omitempty"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

func decodeMutation(
	w http.ResponseWriter,
	r *http.Request,
) (kvMutationRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var input kvMutationRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&input); err != nil {
		return input, err
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return input, errors.New("expected exactly one JSON object")
	}

	if input.ClientID == "" || input.RequestID == "" {
		return input, errors.New("client_id and request_id are required")
	}

	return input, nil
}

func submitMutation(
	w http.ResponseWriter,
	r *http.Request,
	node *shard.Shard,
	command kvstore.Command,
) {
	if err := command.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	status, err := node.Status()
	if err != nil {
		writeKVError(w, node, err)
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

	// Return the previously applied result for an identical request.
	result, found, err := node.CheckedRequestResult(command)
	if err != nil {
		writeKVError(w, node, err)
		return
	}

	if found {
		writeJSON(w, http.StatusOK, result)
		return
	}

	// A proposal is not a commit acknowledgement.
	err = node.Propose(command)

	if err != nil && !errors.Is(err, shard.ErrRequestPending) {
		if errors.Is(err, raft.ErrNotLeader) ||
			errors.Is(err, kvstore.ErrRequestConflict) {
			writeKVError(w, node, err)
		} else {
			writeUnknownMutation(w, node)
		}
		return
	}

	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()

	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()

	for {
		result, found, err := node.CheckedRequestResult(command)

		if err != nil {
			if errors.Is(err, kvstore.ErrRequestConflict) {
				writeKVError(w, node, err)
			} else {
				writeUnknownMutation(w, node)
			}
			return
		}

		// Acknowledge only a matching applied request.
		if found {
			writeJSON(w, http.StatusOK, result)
			return
		}

		select {
		case <-r.Context().Done():
			writeUnknownMutation(w, node)
			return

		case <-timeout.C:
			writeUnknownMutation(w, node)
			return

		case <-poll.C:
		}
	}
}

func registerKVMutationHandlers(
	mux *http.ServeMux,
	node *shard.Shard,
) {
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		input, err := decodeMutation(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		submitMutation(w, r, node, kvstore.Command{
			Op:        kvstore.OpDelete,
			Key:       key,
			ClientID:  input.ClientID,
			RequestID: input.RequestID,
		})
	})

	mux.HandleFunc("POST /kv/{key}/cas", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		input, err := decodeMutation(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		submitMutation(w, r, node, kvstore.Command{
			Op:        kvstore.OpCompareAndSwap,
			Key:       key,
			Expected:  input.Expected,
			Value:     input.Value,
			ClientID:  input.ClientID,
			RequestID: input.RequestID,
		})
	})
}
