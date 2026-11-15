package transport

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestHTTPTransportDelivery(t *testing.T) {
	received := make(chan raft.Message, 1)

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/raft/message" {
				http.NotFound(w, r)
				return
			}

			var message raft.Message
			if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			received <- message
			w.WriteHeader(http.StatusAccepted)
		},
	))
	defer server.Close()

	transport, err := NewHTTPTransport([]Peer{
		{ID: 2, Address: strings.TrimPrefix(server.URL, "http://")},
	}, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	message := raft.Message{
		Type: raft.MsgVote,
		From: 1,
		To:   2,
		Term: 3,
	}

	if err := transport.Send([]raft.Message{message}); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-received:
		if got.Type != message.Type ||
			got.From != message.From ||
			got.To != message.To ||
			got.Term != message.Term {
			t.Fatalf("unexpected message: %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Raft message")
	}
}

func TestHTTPTransportUnknownPeer(t *testing.T) {
	transport, err := NewHTTPTransport(nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	err = transport.Send([]raft.Message{{From: 1, To: 99}})
	if !errors.Is(err, ErrPeer) {
		t.Fatalf("expected ErrPeer, got %v", err)
	}
}

func TestHTTPTransportClosed(t *testing.T) {
	transport, err := NewHTTPTransport(nil, 4)
	if err != nil {
		t.Fatal(err)
	}

	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}

	err = transport.Send(nil)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestHTTPTransportInvalidQueue(t *testing.T) {
	_, err := NewHTTPTransport(nil, 0)
	if err == nil {
		t.Fatal("expected error for zero queue size")
	}
}
