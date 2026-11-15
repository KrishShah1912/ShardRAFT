package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/shard"
)

type raftMessageHandler interface {
	Step(raft.Message) error
	Status() (raft.Status, error)
}

func registerRaftHandlers(
	mux *http.ServeMux,
	node raftMessageHandler,
) {
	mux.HandleFunc("POST /raft/message", func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		defer r.Body.Close()

		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()

		var message raft.Message
		if err := decoder.Decode(&message); err != nil {
			http.Error(w, "invalid Raft message", http.StatusBadRequest)
			return
		}

		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			http.Error(w, "multiple JSON values", http.StatusBadRequest)
			return
		}

		status, err := node.Status()
		if err != nil {
			http.Error(w, "node unavailable", http.StatusServiceUnavailable)
			return
		}

		if message.From == 0 ||
			message.To == 0 ||
			message.To != status.ID ||
			message.From == status.ID ||
			message.Type > raft.MsgHeartbeatResp {
			http.Error(w, "invalid Raft message", http.StatusBadRequest)
			return
		}

		if err := node.Step(message); err != nil {
			if errors.Is(err, shard.ErrClosed) ||
				errors.Is(err, shard.ErrStopped) {
				http.Error(w, "node unavailable", http.StatusServiceUnavailable)
				return
			}

			http.Error(w, "Raft processing failed", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})
}
