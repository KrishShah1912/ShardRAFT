package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestHTTPTransportRetriesTemporaryFailure(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			count := attempts.Add(1)

			if count < 3 {
				http.Error(w, "temporary failure", http.StatusServiceUnavailable)
				return
			}

			w.WriteHeader(http.StatusAccepted)
		},
	))
	defer server.Close()

	transport, err := NewHTTPTransport([]Peer{
		{ID: 2, Address: strings.TrimPrefix(server.URL, "http://")},
	}, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	err = transport.deliverWithRetry(
		context.Background(),
		outgoing{
			message: raft.Message{
				Type: raft.MsgVote,
				From: 1,
				To:   2,
				Term: 1,
			},
			address: strings.TrimPrefix(server.URL, "http://"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

func TestHTTPTransportStopsAfterRetryLimit(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		},
	))
	defer server.Close()

	transport, err := NewHTTPTransport([]Peer{
		{ID: 2, Address: strings.TrimPrefix(server.URL, "http://")},
	}, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	err = transport.deliverWithRetry(
		context.Background(),
		outgoing{
			message: raft.Message{
				Type: raft.MsgAppend,
				From: 1,
				To:   2,
				Term: 1,
			},
			address: strings.TrimPrefix(server.URL, "http://"),
		},
	)

	if err == nil {
		t.Fatal("expected retry exhaustion error")
	}

	if got := attempts.Load(); got != maxDeliveryAttempts {
		t.Fatalf(
			"expected %d attempts, got %d",
			maxDeliveryAttempts,
			got,
		)
	}
}
