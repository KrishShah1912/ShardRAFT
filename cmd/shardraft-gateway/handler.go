package main

import (
	"encoding/json"
	"net/http"
	"time"
)

type gatewayHandler struct {
	client  gatewayKVClient
	timeout time.Duration
}

type gatewayErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func newGatewayHandler(
	c gatewayKVClient,
	timeout time.Duration,
) *gatewayHandler {
	return &gatewayHandler{
		client:  c,
		timeout: timeout,
	}
}

func (h *gatewayHandler) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /kv/{key}", h.handleGet)
	mux.HandleFunc("PUT /kv/{key}", h.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", h.handleDelete)
	mux.HandleFunc("POST /kv/{key}/cas", h.handleCAS)

	return mux
}

func gatewayWriteJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func gatewayWriteError(
	w http.ResponseWriter,
	status int,
	code string,
	message string,
) {
	gatewayWriteJSON(w, status, gatewayErrorResponse{
		Error: message,
		Code:  code,
	})
}
