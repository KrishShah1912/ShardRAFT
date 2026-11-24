package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

const retryTestPayload = `{"value":"Go","client_id":"retry-client","request_id":"retry-request"}`

func retryTestTarget(
	server *httptest.Server,
) leaderTarget {
	return leaderTarget{
		ID:   2,
		Term: 5,
		Address: strings.TrimPrefix(
			server.URL,
			"http://",
		),
	}
}

func retryTestResponse(
	t *testing.T,
	rec *httptest.ResponseRecorder,
	expectedStatus int,
	expectedCode string,
) {
	t.Helper()

	if rec.Code != expectedStatus {
		t.Fatalf(
			"status=%d, want %d; body=%s",
			rec.Code,
			expectedStatus,
			rec.Body.String(),
		)
	}

	if expectedCode == "" {
		return
	}

	var result routingErrorResponse

	if err := json.Unmarshal(
		rec.Body.Bytes(),
		&result,
	); err != nil {
		t.Fatal(err)
	}

	if result.Code != expectedCode {
		t.Fatalf(
			"code=%s, want %s",
			result.Code,
			expectedCode,
		)
	}
}

func TestRequestRetryLeaderRediscovery(t *testing.T) {
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32

	first := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			firstCalls.Add(1)

			writeRoutingError(
				w,
				http.StatusServiceUnavailable,
				routingCodeNotLeader,
				"leadership changed",
				raft.Status{
					Term:     6,
					LeaderID: 3,
				},
			)
		}),
	)
	defer first.Close()

	second := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			secondCalls.Add(1)

			writeJSON(
				w,
				http.StatusOK,
				map[string]bool{
					"applied": true,
				},
			)
		}),
	)
	defer second.Close()

	discoveries := 0

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			discoveries++

			if discoveries == 1 {
				return retryTestTarget(first), nil
			}

			target := retryTestTarget(second)
			target.ID = 3
			target.Term = 6

			return target, nil
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t,
		router,
		http.MethodPut,
		"/kv/retry",
		retryTestPayload,
	)

	retryTestResponse(t, rec, http.StatusOK, "")

	if discoveries != 2 ||
		firstCalls.Load() != 1 ||
		secondCalls.Load() != 1 {
		t.Fatalf(
			"discoveries=%d first=%d second=%d",
			discoveries,
			firstCalls.Load(),
			secondCalls.Load(),
		)
	}
}

func TestRequestRetryPreservesIdentity(t *testing.T) {
	var calls atomic.Int32

	server := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			body, err := io.ReadAll(r.Body)

			if err != nil {
				t.Error(err)
			}

			if string(body) != retryTestPayload {
				t.Errorf(
					"payload changed: %s",
					body,
				)
			}

			if r.Method != http.MethodPut ||
				r.URL.Path != "/kv/retry" {
				t.Error("method or path changed")
			}

			if r.Header.Get(
				routingForwardHeader,
			) != "1" {
				t.Error("missing forwarding header")
			}

			if calls.Add(1) < 3 {
				writeRoutingError(
					w,
					http.StatusGatewayTimeout,
					routingCodeUnknown,
					"outcome unknown",
					raft.Status{},
				)
				return
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
			return retryTestTarget(server), nil
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t,
		router,
		http.MethodPut,
		"/kv/retry",
		retryTestPayload,
	)

	retryTestResponse(t, rec, http.StatusOK, "")

	if calls.Load() != 3 {
		t.Fatalf(
			"calls=%d, want 3",
			calls.Load(),
		)
	}
}

func TestRequestRetryExhaustion(t *testing.T) {
	discoveries := 0

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			discoveries++

			return leaderTarget{},
				errLeaderUnavailable
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t,
		router,
		http.MethodGet,
		"/kv/retry",
		"",
	)

	retryTestResponse(
		t,
		rec,
		http.StatusServiceUnavailable,
		routingCodeUnavailable,
	)

	if discoveries != routingMaxAttempts {
		t.Fatalf(
			"discoveries=%d, want %d",
			discoveries,
			routingMaxAttempts,
		)
	}
}

func TestRequestRetryPreservesUnknownOutcome(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			writeRoutingError(
				w,
				http.StatusGatewayTimeout,
				routingCodeUnknown,
				"commit not confirmed",
				raft.Status{},
			)
		}),
	)
	defer server.Close()

	discoveries := 0

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			discoveries++

			if discoveries == 1 {
				return retryTestTarget(server), nil
			}

			return leaderTarget{},
				errLeaderUnavailable
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t,
		router,
		http.MethodPut,
		"/kv/retry",
		retryTestPayload,
	)

	retryTestResponse(
		t,
		rec,
		http.StatusGatewayTimeout,
		routingCodeUnknown,
	)

	if discoveries != routingMaxAttempts {
		t.Fatalf(
			"discoveries=%d",
			discoveries,
		)
	}
}

func TestRequestRetryDefinitiveResponses(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{"success", http.StatusOK},
		{"invalid", http.StatusBadRequest},
		{"missing", http.StatusNotFound},
		{"conflict", http.StatusConflict},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32

			server := httptest.NewServer(
				http.HandlerFunc(func(
					w http.ResponseWriter,
					r *http.Request,
				) {
					calls.Add(1)

					writeJSON(
						w,
						tc.status,
						map[string]bool{
							"applied": false,
						},
					)
				}),
			)
			defer server.Close()

			router := newRequestRouter(
				http.NotFoundHandler(),
				func(context.Context) (leaderTarget, error) {
					return retryTestTarget(server), nil
				},
				routerFollowerStatus,
			)

			rec := sendKVRequest(
				t,
				router,
				http.MethodPut,
				"/kv/retry",
				retryTestPayload,
			)

			retryTestResponse(
				t,
				rec,
				tc.status,
				"",
			)

			if calls.Load() != 1 {
				t.Fatalf(
					"calls=%d, want 1",
					calls.Load(),
				)
			}
		})
	}
}

func TestRequestRetryRequiresMutationIdentity(t *testing.T) {
	discoveries := 0

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			discoveries++

			return leaderTarget{},
				errLeaderUnavailable
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t,
		router,
		http.MethodPut,
		"/kv/retry",
		`{"value":"Go"}`,
	)

	retryTestResponse(
		t,
		rec,
		http.StatusServiceUnavailable,
		routingCodeUnavailable,
	)

	if discoveries != 1 {
		t.Fatalf(
			"discoveries=%d, want 1",
			discoveries,
		)
	}
}

func TestRequestRetryCanceledContext(t *testing.T) {
	discoveries := 0

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			discoveries++

			return leaderTarget{},
				errLeaderUnavailable
		},
		routerFollowerStatus,
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	req := httptest.NewRequest(
		http.MethodGet,
		"/kv/retry",
		nil,
	).WithContext(ctx)

	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if discoveries != 0 {
		t.Fatalf(
			"discoveries=%d, want 0",
			discoveries,
		)
	}
}

func TestRequestRetryCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	discoveries := 0

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			discoveries++
			cancel()

			return leaderTarget{},
				errLeaderUnavailable
		},
		routerFollowerStatus,
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/kv/retry",
		nil,
	).WithContext(ctx)

	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if discoveries != 1 {
		t.Fatalf(
			"discoveries=%d, want 1",
			discoveries,
		)
	}
}

func TestRequestRetryLocalLeadershipChange(t *testing.T) {
	localCalls := 0
	discoveries := 0

	server := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
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

	local := http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		localCalls++

		writeRoutingError(
			w,
			http.StatusServiceUnavailable,
			routingCodeNotLeader,
			"leadership changed",
			raft.Status{
				Term:     6,
				LeaderID: 2,
			},
		)
	})

	router := newRequestRouter(
		local,
		func(context.Context) (leaderTarget, error) {
			discoveries++
			return retryTestTarget(server), nil
		},
		func() (raft.Status, error) {
			if localCalls == 0 {
				return raft.Status{
					ID:       1,
					Role:     raft.Leader,
					Term:     5,
					LeaderID: 1,
				}, nil
			}

			return routerFollowerStatus()
		},
	)

	rec := sendKVRequest(
		t,
		router,
		http.MethodPut,
		"/kv/retry",
		retryTestPayload,
	)

	retryTestResponse(t, rec, http.StatusOK, "")

	if localCalls != 1 || discoveries != 1 {
		t.Fatalf(
			"local=%d discoveries=%d",
			localCalls,
			discoveries,
		)
	}
}
