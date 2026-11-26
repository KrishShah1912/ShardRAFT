package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

type controller struct {
	mu sync.Mutex
}

func respond(
	w http.ResponseWriter,
	status int,
	data any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func compose(
	ctx context.Context,
	args ...string,
) ([]byte, error) {
	base := []string{
		"compose",
		"-f", "docker-compose.yml",
		"-p", "shardraft",
	}

	return exec.CommandContext(
		ctx,
		"docker",
		append(base, args...)...,
	).CombinedOutput()
}

func (c *controller) status(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx, cancel := context.WithTimeout(
		r.Context(),
		10*time.Second,
	)
	defer cancel()

	output, err := compose(
		ctx,
		"ps",
		"-a",
		"--format",
		"json",
	)

	if err != nil {
		respond(w, 503, map[string]any{
			"error":  err.Error(),
			"output": string(output),
		})
		return
	}

	respond(w, 200, map[string]any{
		"mode":   "docker",
		"output": string(output),
	})
}

func (c *controller) action(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := strconv.Atoi(r.PathValue("id"))

	if err != nil || id < 1 || id > 5 {
		respond(w, 400, map[string]any{
			"error": "invalid storage node",
		})
		return
	}

	action := r.PathValue("action")

	switch action {
	case "start", "stop", "restart":
	default:
		respond(w, 400, map[string]any{
			"error": "unsupported action",
		})
		return
	}

	if !c.mu.TryLock() {
		respond(w, 409, map[string]any{
			"error": "another control operation is running",
		})
		return
	}
	defer c.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		r.Context(),
		60*time.Second,
	)
	defer cancel()

	service := fmt.Sprintf("n%d", id)

	output, err := compose(ctx, action, service)

	if err != nil {
		respond(w, 503, map[string]any{
			"node_id": id,
			"action":  action,
			"error":   err.Error(),
			"output":  string(output),
		})
		return
	}

	respond(w, 200, map[string]any{
		"node_id": id,
		"action":  action,
		"success": true,
		"output":  string(output),
	})
}

func newHandler() http.Handler {
	c := &controller{}
	mux := http.NewServeMux()

	mux.HandleFunc(
		"GET /health",
		func(w http.ResponseWriter, r *http.Request) {
			respond(w, 200, map[string]any{
				"status": "ok",
				"mode":   "docker",
			})
		},
	)

	mux.HandleFunc(
		"GET /api/v1/control/cluster",
		c.status,
	)

	mux.HandleFunc(
		"POST /api/v1/control/nodes/{id}/{action}",
		c.action,
	)

	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// Reject cross-origin browser requests.
			origin := r.Header.Get("Origin")

			switch origin {
			case "",
				"http://localhost:5173",
				"http://127.0.0.1:5173",
				"http://127.0.0.1:18090":
			default:
				respond(w, 403, map[string]any{
					"error": "origin not allowed",
				})
				return
			}

			mux.ServeHTTP(w, r)
		},
	)
}

func main() {
	server := &http.Server{
		Addr:              "127.0.0.1:18090",
		Handler:           newHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      70 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	log.Printf(
		"Docker control listening on http://%s",
		server.Addr,
	)

	log.Fatal(server.ListenAndServe())
}
