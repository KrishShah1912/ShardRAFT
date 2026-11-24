package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestRoutingErrorNotLeader(t *testing.T) {
	rec := httptest.NewRecorder()

	writeRoutingError(
		rec,
		http.StatusServiceUnavailable,
		routingCodeNotLeader,
		"not leader",
		raft.Status{
			LeaderID: 2,
			Term:     5,
		},
	)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rec.Code)
	}

	var result routingErrorResponse

	if err := json.Unmarshal(
		rec.Body.Bytes(), &result,
	); err != nil {
		t.Fatal(err)
	}

	if result.Code != routingCodeNotLeader {
		t.Fatalf("unexpected code: %s", result.Code)
	}

	if result.LeaderID != 2 || result.Term != 5 {
		t.Fatalf("unexpected routing response: %+v", result)
	}
}

func TestRoutingErrorLeaderUnavailable(t *testing.T) {
	rec := httptest.NewRecorder()

	writeRoutingError(
		rec,
		http.StatusServiceUnavailable,
		routingCodeUnavailable,
		"leader unavailable",
		raft.Status{},
	)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rec.Code)
	}

	var result routingErrorResponse

	if err := json.Unmarshal(
		rec.Body.Bytes(), &result,
	); err != nil {
		t.Fatal(err)
	}

	if result.Code != routingCodeUnavailable {
		t.Fatalf("unexpected code: %s", result.Code)
	}

	if result.LeaderID != 0 {
		t.Fatalf("expected unknown leader")
	}
}

func TestRoutingErrorOutcomeUnknown(t *testing.T) {
	rec := httptest.NewRecorder()

	writeRoutingError(
		rec,
		http.StatusGatewayTimeout,
		routingCodeUnknown,
		"commit not confirmed; request outcome unknown",
		raft.Status{},
	)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d, want 504", rec.Code)
	}

	var result routingErrorResponse

	if err := json.Unmarshal(
		rec.Body.Bytes(), &result,
	); err != nil {
		t.Fatal(err)
	}

	if result.Code != routingCodeUnknown {
		t.Fatalf("unexpected code: %s", result.Code)
	}
}
