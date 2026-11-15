package transport

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestHTTPTransportReportsDeliveryFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(
				w,
				"peer unavailable",
				http.StatusServiceUnavailable,
			)
		},
	))
	defer server.Close()

	transport, err := NewHTTPTransport([]Peer{
		{
			ID:      2,
			Address: strings.TrimPrefix(server.URL, "http://"),
		},
	}, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	err = transport.Send([]raft.Message{
		{
			Type: raft.MsgAppend,
			From: 1,
			To:   2,
			Term: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-transport.Errors():
		if err == nil {
			t.Fatal("expected non-nil delivery error")
		}

	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delivery failure")
	}
}

func TestHTTPTransportDoesNotReportSuccessfulDelivery(t *testing.T) {
	delivered := make(chan struct{}, 1)

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			delivered <- struct{}{}
			w.WriteHeader(http.StatusAccepted)
		},
	))
	defer server.Close()

	transport, err := NewHTTPTransport([]Peer{
		{
			ID:      2,
			Address: strings.TrimPrefix(server.URL, "http://"),
		},
	}, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	if err := transport.Send([]raft.Message{
		{
			Type: raft.MsgVote,
			From: 1,
			To:   2,
			Term: 1,
		},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-delivered:
	case <-time.After(3 * time.Second):
		t.Fatal("message was not delivered")
	}

	select {
	case err := <-transport.Errors():
		t.Fatalf("unexpected delivery error: %v", err)

	case <-time.After(100 * time.Millisecond):
	}
}
