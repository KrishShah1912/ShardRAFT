package transport

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestPeerIsolation(t *testing.T) {
	blocked := make(chan struct{})
	started := make(chan struct{}, 1)

	slow := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-blocked
			w.WriteHeader(http.StatusAccepted)
		},
	))
	defer slow.Close()
	defer close(blocked)

	received := make(chan raft.Message, 1)
	fast := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			received <- raft.Message{}
			w.WriteHeader(http.StatusAccepted)
		},
	))
	defer fast.Close()

	transport, err := NewHTTPTransport([]Peer{
		{ID: 2, Address: strings.TrimPrefix(slow.URL, "http://")},
		{ID: 3, Address: strings.TrimPrefix(fast.URL, "http://")},
	}, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	if err := transport.Send([]raft.Message{
		{Type: raft.MsgAppend, From: 1, To: 2},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("slow peer request did not start")
	}

	if err := transport.Send([]raft.Message{
		{Type: raft.MsgVote, From: 1, To: 3},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-received:
		// Healthy peer received its message independently.
	case <-time.After(1 * time.Second):
		t.Fatal("healthy peer was blocked by slow peer")
	}
}
