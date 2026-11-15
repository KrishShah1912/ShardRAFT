package transport

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestFullPeerQueueDoesNotBlockHealthyPeer(t *testing.T) {
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

	received := make(chan struct{}, 1)

	fast := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case received <- struct{}{}:
			default:
			}
			w.WriteHeader(http.StatusAccepted)
		},
	))
	defer fast.Close()

	tr, err := NewHTTPTransport([]Peer{
		{ID: 2, Address: strings.TrimPrefix(slow.URL, "http://")},
		{ID: 3, Address: strings.TrimPrefix(fast.URL, "http://")},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	// Occupy the slow peer's worker.
	if err := tr.Send([]raft.Message{
		{Type: raft.MsgAppend, From: 1, To: 2},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("slow peer request did not start")
	}

	// Fill its only available queue slot.
	if err := tr.Send([]raft.Message{
		{Type: raft.MsgAppend, From: 1, To: 2},
	}); err != nil {
		t.Fatal(err)
	}

	// A full queue for peer 2 must not prevent peer 3
	// from receiving its message.
	err = tr.Send([]raft.Message{
		{Type: raft.MsgAppend, From: 1, To: 2},
		{Type: raft.MsgVote, From: 1, To: 3},
	})

	if err != nil && !errors.Is(err, ErrQueueFull) {
		t.Fatalf("unexpected send error: %v", err)
	}

	select {
	case <-received:
		// Healthy peer received its message.
	case <-time.After(2 * time.Second):
		t.Fatal("healthy peer was blocked by full peer queue")
	}
}
