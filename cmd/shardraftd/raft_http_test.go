package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type fakeRaftReceiver struct {
	messages []raft.Message
	stepErr  error
}

func (f *fakeRaftReceiver) Step(message raft.Message) error {
	if f.stepErr != nil {
		return f.stepErr
	}
	f.messages = append(f.messages, message)
	return nil
}

func (f *fakeRaftReceiver) Status() (raft.Status, error) {
	return raft.Status{ID: 2}, nil
}

func raftReceiverRequest(
	t *testing.T,
	handler http.Handler,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(
		http.MethodPost,
		"/raft/message",
		bytes.NewReader(body),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestRaftHTTPReceivesMessage(t *testing.T) {
	node := &fakeRaftReceiver{}
	mux := http.NewServeMux()
	registerRaftHandlers(mux, node)

	message := raft.Message{
		Type: raft.MsgVote,
		From: 1,
		To:   2,
		Term: 3,
	}

	body, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}

	rec := raftReceiverRequest(t, mux, body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s",
			rec.Code, rec.Body.String())
	}

	if len(node.messages) != 1 {
		t.Fatalf("expected one message, got %d", len(node.messages))
	}

	if node.messages[0].Term != 3 {
		t.Fatalf("unexpected message: %+v", node.messages[0])
	}
}

func TestRaftHTTPRejectsInvalidMessages(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"invalid JSON", "{"},
		{"unknown field", `{"Unknown":123}`},
		{"multiple values", `{}` + "\n" + `{}`},
		{"missing sender", `{"To":2}`},
		{"wrong destination", `{"From":1,"To":3}`},
		{"self message", `{"From":2,"To":2}`},
		{"invalid type", `{"Type":255,"From":1,"To":2}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &fakeRaftReceiver{}
			mux := http.NewServeMux()
			registerRaftHandlers(mux, node)

			rec := raftReceiverRequest(
				t, mux, []byte(tc.body),
			)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s",
					rec.Code, rec.Body.String())
			}

			if len(node.messages) != 0 {
				t.Fatal("invalid message was processed")
			}
		})
	}
}

func TestRaftHTTPProcessingFailure(t *testing.T) {
	node := &fakeRaftReceiver{
		stepErr: errors.New("injected failure"),
	}
	mux := http.NewServeMux()
	registerRaftHandlers(mux, node)

	rec := raftReceiverRequest(
		t,
		mux,
		[]byte(`{"Type":0,"From":1,"To":2,"Term":1}`),
	)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
