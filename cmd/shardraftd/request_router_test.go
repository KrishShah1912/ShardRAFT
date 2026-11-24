package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func routerFollowerStatus() (raft.Status, error) {
	return raft.Status{
		ID:       1,
		Role:     raft.Follower,
		Term:     5,
		LeaderID: 2,
	}, nil
}

func TestRequestRouterLocalLeader(t *testing.T) {
	local := http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("local"))
	})

	router := newRequestRouter(
		local,
		func(context.Context) (leaderTarget, error) {
			t.Fatal("leader discovery should not run")
			return leaderTarget{}, nil
		},
		func() (raft.Status, error) {
			return raft.Status{
				ID:       1,
				Role:     raft.Leader,
				Term:     5,
				LeaderID: 1,
			}, nil
		},
	)

	rec := sendKVRequest(
		t, router,
		http.MethodGet, "/kv/test", "",
	)

	if rec.Code != http.StatusOK ||
		rec.Body.String() != "local" {
		t.Fatalf("unexpected response: %d %s",
			rec.Code, rec.Body.String())
	}
}

func TestRequestRouterForwardOperations(t *testing.T) {
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{
			http.MethodGet,
			"/kv/language",
			"",
		},
		{
			http.MethodPut,
			"/kv/language",
			`{"value":"Go","client_id":"c1","request_id":"r1"}`,
		},
		{
			http.MethodDelete,
			"/kv/language",
			`{"client_id":"c1","request_id":"r2"}`,
		},
		{
			http.MethodPost,
			"/kv/language/cas",
			`{"expected":"Go","value":"Python","client_id":"c1","request_id":"r3"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			server := httptest.NewServer(
				http.HandlerFunc(func(
					w http.ResponseWriter,
					r *http.Request,
				) {
					if r.Method != tc.method {
						t.Errorf("method=%s", r.Method)
					}

					if r.URL.Path != tc.path {
						t.Errorf("path=%s", r.URL.Path)
					}

					if r.Header.Get(
						routingForwardHeader,
					) != "1" {
						t.Error("missing forwarding header")
					}

					body, err := io.ReadAll(r.Body)

					if err != nil ||
						string(body) != tc.body {
						t.Errorf("unexpected body: %s", body)
					}

					writeJSON(
						w,
						http.StatusOK,
						map[string]bool{
							"applied": true,
						},
					)
				}),
			)
			defer server.Close()

			router := newRequestRouter(
				http.NotFoundHandler(),
				func(context.Context) (leaderTarget, error) {
					return leaderTarget{
						ID:   2,
						Term: 5,
						Address: strings.TrimPrefix(
							server.URL,
							"http://",
						),
					}, nil
				},
				routerFollowerStatus,
			)

			rec := sendKVRequest(
				t, router,
				tc.method, tc.path, tc.body,
			)

			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s",
					rec.Code, rec.Body.String())
			}

			if rec.Body.String() != "{\"applied\":true}\n" {
				t.Fatalf("unexpected response: %s",
					rec.Body.String())
			}
		})
	}
}

func TestRequestRouterPreventsForwardingLoop(t *testing.T) {
	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			t.Fatal("forwarded request must not rediscover")
			return leaderTarget{}, nil
		},
		routerFollowerStatus,
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/kv/test",
		nil,
	)

	req.Header.Set(routingForwardHeader, "1")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

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
}

func TestRequestRouterLeaderUnavailable(t *testing.T) {
	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			return leaderTarget{}, errLeaderUnavailable
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t, router,
		http.MethodGet, "/kv/test", "",
	)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rec.Code)
	}
}

func TestRequestRouterNonKVBypass(t *testing.T) {
	local := http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("status"))
	})

	router := newRequestRouter(
		local,
		func(context.Context) (leaderTarget, error) {
			t.Fatal("non-KV request must bypass routing")
			return leaderTarget{}, nil
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t, router,
		http.MethodGet, "/status", "",
	)

	if rec.Code != http.StatusOK ||
		rec.Body.String() != "status" {
		t.Fatalf("unexpected response: %d %s",
			rec.Code, rec.Body.String())
	}
}

func TestRequestRouterPreservesConflict(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			http.Error(
				w,
				"request conflict",
				http.StatusConflict,
			)
		}),
	)
	defer server.Close()

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			return leaderTarget{
				ID: 2,
				Address: strings.TrimPrefix(
					server.URL, "http://",
				),
			}, nil
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t, router,
		http.MethodPut,
		"/kv/test",
		`{"value":"Go","client_id":"c1","request_id":"r1"}`,
	)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d, want 409", rec.Code)
	}
}

func TestRequestRouterUnknownMutationOutcome(t *testing.T) {
	server := httptest.NewServer(
		http.NotFoundHandler(),
	)

	address := strings.TrimPrefix(
		server.URL, "http://",
	)

	server.Close()

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			return leaderTarget{
				ID:      2,
				Address: address,
			}, nil
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t, router,
		http.MethodPut,
		"/kv/test",
		`{"value":"Go","client_id":"c1","request_id":"r1"}`,
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
