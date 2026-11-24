package main

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/observability"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestEventHistoryHandler(t *testing.T) {
	bus := observability.NewBus(10)

	bus.Publish(observability.Event{
		NodeID:   raft.NodeID(1),
		Category: observability.CategoryNode,
		Type:     observability.EventNodeStarted,
	})

	bus.Publish(observability.Event{
		NodeID:   raft.NodeID(1),
		Category: observability.CategoryRaft,
		Type:     observability.EventRaftLeaderElected,
	})

	mux := http.NewServeMux()
	registerEventHandlers(mux, bus)

	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/events/history?after=1",
		nil,
	)

	recorder := httptest.NewRecorder()

	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"expected status 200, got %d",
			recorder.Code,
		)
	}

	body := recorder.Body.String()

	if !strings.Contains(
		body,
		observability.EventRaftLeaderElected,
	) {
		t.Fatalf(
			"expected leader event in response: %s",
			body,
		)
	}

	if strings.Contains(
		body,
		observability.EventNodeStarted,
	) {
		t.Fatalf(
			"history returned event before requested ID: %s",
			body,
		)
	}
}

func TestEventHistoryRejectsInvalidAfter(t *testing.T) {
	bus := observability.NewBus(10)

	mux := http.NewServeMux()
	registerEventHandlers(mux, bus)

	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/events/history?after=bad",
		nil,
	)

	recorder := httptest.NewRecorder()

	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected 400, got %d",
			recorder.Code,
		)
	}
}

func TestSSEStreamsPublishedEvent(t *testing.T) {
	bus := observability.NewBus(10)

	mux := http.NewServeMux()
	registerEventHandlers(mux, bus)

	server := httptest.NewServer(mux)
	defer server.Close()

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	response, err := client.Get(
		server.URL + "/api/v1/events",
	)
	if err != nil {
		t.Fatalf("open SSE stream: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf(
			"expected 200, got %d",
			response.StatusCode,
		)
	}

	go bus.Publish(observability.Event{
		NodeID:   raft.NodeID(1),
		Category: observability.CategoryRaft,
		Type:     observability.EventRaftElectionStarted,
	})

	reader := bufio.NewReader(response.Body)

	foundEvent := false
	foundData := false

	deadline := time.After(1500 * time.Millisecond)

	for !(foundEvent && foundData) {
		select {
		case <-deadline:
			t.Fatal("timed out reading SSE event")
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE stream: %v", err)
		}

		if strings.Contains(
			line,
			"event: "+observability.EventRaftElectionStarted,
		) {
			foundEvent = true
		}

		if strings.HasPrefix(line, "data: ") &&
			strings.Contains(
				line,
				observability.EventRaftElectionStarted,
			) {
			foundData = true
		}
	}
}
