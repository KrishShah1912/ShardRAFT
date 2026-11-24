package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/transport"
)

func failoverLocalLeader() (raft.Status, error) {
	return raft.Status{
		ID:       1,
		Role:     raft.Leader,
		Term:     5,
		LeaderID: 1,
	}, nil
}

func TestRequestFailoverStaleLocalLeader(t *testing.T) {
	peer := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       2,
			Role:     raft.Leader,
			Term:     6,
			LeaderID: 2,
		},
	)

	discovery := newLeaderDiscovery(
		1,
		[]transport.Peer{peer},
		failoverLocalLeader,
	)

	target, err := discovery.discover(
		context.Background(),
	)

	if err != nil {
		t.Fatal(err)
	}

	if target.ID != 2 ||
		target.Term != 6 ||
		target.Local {
		t.Fatalf(
			"unexpected target: %+v",
			target,
		)
	}
}

func TestRequestFailoverHigherTermWithoutLeader(t *testing.T) {
	peer := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:   2,
			Role: raft.Candidate,
			Term: 6,
		},
	)

	discovery := newLeaderDiscovery(
		1,
		[]transport.Peer{peer},
		failoverLocalLeader,
	)

	_, err := discovery.discover(
		context.Background(),
	)

	if !errors.Is(err, errLeaderUnavailable) {
		t.Fatalf(
			"expected leader unavailable, got %v",
			err,
		)
	}
}

func TestRequestFailoverUnreachableOldLeader(t *testing.T) {
	oldServer := httptest.NewServer(
		http.NotFoundHandler(),
	)

	oldAddress := strings.TrimPrefix(
		oldServer.URL,
		"http://",
	)

	oldServer.Close()

	newLeader := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       3,
			Role:     raft.Leader,
			Term:     6,
			LeaderID: 3,
		},
	)

	discovery := newLeaderDiscovery(
		1,
		[]transport.Peer{
			{
				ID:      2,
				Address: oldAddress,
			},
			newLeader,
		},
		routerFollowerStatus,
	)

	target, err := discovery.discover(
		context.Background(),
	)

	if err != nil {
		t.Fatal(err)
	}

	if target.ID != 3 ||
		target.Term != 6 {
		t.Fatalf(
			"unexpected target: %+v",
			target,
		)
	}
}

func TestRequestFailoverConflictingLocalLeader(t *testing.T) {
	peer := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       2,
			Role:     raft.Leader,
			Term:     5,
			LeaderID: 2,
		},
	)

	discovery := newLeaderDiscovery(
		1,
		[]transport.Peer{peer},
		failoverLocalLeader,
	)

	_, err := discovery.discover(
		context.Background(),
	)

	if !errors.Is(err, errLeaderUnavailable) {
		t.Fatalf(
			"expected conflicting leaders to be rejected, got %v",
			err,
		)
	}
}

func TestRequestFailoverRouterAvoidsStaleLocalLeader(t *testing.T) {
	localCalls := 0

	server := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			writeJSON(
				w,
				http.StatusOK,
				map[string]string{
					"value": "new-leader",
				},
			)
		}),
	)
	defer server.Close()

	router := newRequestRouter(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			localCalls++
			http.Error(
				w,
				"stale leader",
				http.StatusInternalServerError,
			)
		}),
		func(context.Context) (leaderTarget, error) {
			return leaderTarget{
				ID:   2,
				Term: 6,
				Address: strings.TrimPrefix(
					server.URL,
					"http://",
				),
			}, nil
		},
		failoverLocalLeader,
	)

	router.verifyLeadership = true

	rec := sendKVRequest(
		t,
		router,
		http.MethodGet,
		"/kv/failover",
		"",
	)

	retryTestResponse(
		t,
		rec,
		http.StatusOK,
		"",
	)

	if localCalls != 0 {
		t.Fatal("stale local leader executed request")
	}

	if rec.Body.String() != "{\"value\":\"new-leader\"}\n" {
		t.Fatalf(
			"unexpected response: %s",
			rec.Body.String(),
		)
	}
}

func TestRequestFailoverForwardedStaleLeader(t *testing.T) {
	localCalls := 0

	router := newRequestRouter(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			localCalls++
			w.WriteHeader(http.StatusOK)
		}),
		func(context.Context) (leaderTarget, error) {
			return leaderTarget{
				ID:      2,
				Term:    6,
				Address: "127.0.0.1:8082",
			}, nil
		},
		failoverLocalLeader,
	)

	router.verifyLeadership = true

	req := httptest.NewRequest(
		http.MethodGet,
		"/kv/failover",
		nil,
	)

	req.Header.Set(
		routingForwardHeader,
		"1",
	)

	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	retryTestResponse(
		t,
		rec,
		http.StatusServiceUnavailable,
		routingCodeNotLeader,
	)

	if localCalls != 0 {
		t.Fatal("forwarded request executed on stale leader")
	}
}

func TestRequestFailoverElectionRecovery(t *testing.T) {
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

	router := newRequestRouter(
		http.NotFoundHandler(),
		func(context.Context) (leaderTarget, error) {
			discoveries++

			if discoveries < 3 {
				return leaderTarget{},
					errLeaderUnavailable
			}

			return leaderTarget{
				ID:   3,
				Term: 6,
				Address: strings.TrimPrefix(
					server.URL,
					"http://",
				),
			}, nil
		},
		routerFollowerStatus,
	)

	rec := sendKVRequest(
		t,
		router,
		http.MethodPut,
		"/kv/failover",
		retryTestPayload,
	)

	retryTestResponse(
		t,
		rec,
		http.StatusOK,
		"",
	)

	if discoveries != 3 {
		t.Fatalf(
			"expected three discoveries, got %d",
			discoveries,
		)
	}
}

func TestRequestFailoverDeadline(t *testing.T) {
	router := newRequestRouter(
		http.NotFoundHandler(),
		func(ctx context.Context) (leaderTarget, error) {
			<-ctx.Done()
			return leaderTarget{}, ctx.Err()
		},
		routerFollowerStatus,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()

	req := httptest.NewRequest(
		http.MethodGet,
		"/kv/failover",
		nil,
	).WithContext(ctx)

	rec := httptest.NewRecorder()

	start := time.Now()

	router.ServeHTTP(rec, req)

	elapsed := time.Since(start)

	retryTestResponse(
		t,
		rec,
		http.StatusServiceUnavailable,
		routingCodeUnavailable,
	)

	if elapsed > 2*time.Second {
		t.Fatalf(
			"request exceeded deadline tolerance: %s",
			elapsed,
		)
	}
}

func TestRequestFailoverVerifiedLocalLeader(t *testing.T) {
	localCalls := 0

	peer := newDiscoveryTestPeer(
		t,
		peerStatusResponse{
			ID:       2,
			Role:     raft.Follower,
			Term:     5,
			LeaderID: 1,
		},
	)

	discovery := newLeaderDiscovery(
		1,
		[]transport.Peer{peer},
		failoverLocalLeader,
	)

	router := newRequestRouter(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			localCalls++

			writeJSON(
				w,
				http.StatusOK,
				map[string]string{
					"value": "local-leader",
				},
			)
		}),
		discovery.discover,
		failoverLocalLeader,
	)

	router.verifyLeadership = true

	rec := sendKVRequest(
		t,
		router,
		http.MethodGet,
		"/kv/failover",
		"",
	)

	retryTestResponse(
		t,
		rec,
		http.StatusOK,
		"",
	)

	if localCalls != 1 {
		t.Fatalf(
			"expected one local execution, got %d",
			localCalls,
		)
	}
}
