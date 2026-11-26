package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/history"
)

func TestGatewayResponseContract(t *testing.T) {
	tests := []struct {
		name    string
		kind    history.Kind
		status  int
		body    string
		outcome string
		found   bool
		applied bool
	}{
		{
			name:    "existing_key",
			kind:    history.Get,
			status:  http.StatusOK,
			body:    `{"key":"key","value":"value","found":true}`,
			outcome: "success",
			found:   true,
		},
		{
			name:    "missing_key",
			kind:    history.Get,
			status:  http.StatusNotFound,
			body:    `{"error":"key not found","code":"key_not_found"}`,
			outcome: "success",
		},
		{
			name:    "put_without_key",
			kind:    history.Put,
			status:  http.StatusOK,
			body:    `{"applied":true,"value":"value","client_id":"client","request_id":"request"}`,
			outcome: "success",
			applied: true,
		},
		{
			name:    "wrong_read_key",
			kind:    history.Get,
			status:  http.StatusOK,
			body:    `{"key":"wrong","value":"value","found":true}`,
			outcome: "failed",
		},
		{
			name:    "unrecognized_404",
			kind:    history.Get,
			status:  http.StatusNotFound,
			body:    `{"error":"not found","code":"unknown"}`,
			outcome: "failed",
		},
		{
			name:    "wrong_mutation_identity",
			kind:    history.Put,
			status:  http.StatusOK,
			body:    `{"applied":true,"value":"value","client_id":"client","request_id":"wrong"}`,
			outcome: "unknown",
		},
		{
			name:    "unapplied_put",
			kind:    history.Put,
			status:  http.StatusOK,
			body:    `{"applied":false,"client_id":"client","request_id":"request"}`,
			outcome: "unknown",
		},
		{
			name:    "unavailable_gateway",
			kind:    history.Put,
			status:  http.StatusServiceUnavailable,
			body:    `{"error":"cluster unavailable","code":"leader_unavailable"}`,
			outcome: "unknown",
		},
		{
			name:    "malformed_response",
			kind:    history.Get,
			status:  http.StatusOK,
			body:    `invalid json`,
			outcome: "failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.status)
					_, _ = fmt.Fprint(w, tc.body)
				},
			))

			defer server.Close()

			out := execute(
				context.Background(),
				server.Client(),
				server.URL,
				history.Input{
					Kind:      tc.kind,
					Key:       "key",
					Value:     "value",
					ClientID:  "client",
					RequestID: "request",
				},
			)

			if string(out.Outcome) != tc.outcome ||
				out.Found != tc.found ||
				out.Applied != tc.applied {
				t.Fatalf("unexpected response: %+v", out)
			}

			if tc.outcome != "success" && out.Error == "" {
				t.Fatal("unsuccessful response lost error details")
			}
		})
	}
}
