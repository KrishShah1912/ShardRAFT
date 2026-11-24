package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

const (
	routingForwardHeader  = "X-ShardRAFT-Forwarded"
	routingMaxBody        = 1 << 20
	routingMaxResponse    = 4 << 20
	routingTimeout        = 10 * time.Second
	routingAttemptTimeout = 4 * time.Second
)

type requestRouter struct {
	local            http.Handler
	discover         func(context.Context) (leaderTarget, error)
	status           func() (raft.Status, error)
	client           *http.Client
	verifyLeadership bool
}

func newRequestRouter(
	local http.Handler,
	discover func(context.Context) (leaderTarget, error),
	status func() (raft.Status, error),
) *requestRouter {
	return &requestRouter{
		local:    local,
		discover: discover,
		status:   status,
		client: &http.Client{
			Timeout: routingAttemptTimeout,
			CheckRedirect: func(
				req *http.Request,
				via []*http.Request,
			) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func isRoutableKVRequest(r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/kv/") {
		return false
	}

	switch r.Method {
	case http.MethodGet,
		http.MethodPut,
		http.MethodDelete,
		http.MethodPost:
		return true
	default:
		return false
	}
}

func (router *requestRouter) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	if !isRoutableKVRequest(r) {
		router.local.ServeHTTP(w, r)
		return
	}

	// A forwarded request may execute locally, but
	// must never initiate another forwarding attempt.
	if r.Header.Get(routingForwardHeader) != "" {
		status, err := router.status()

		if err != nil {
			writeRoutingError(
				w,
				http.StatusServiceUnavailable,
				routingCodeUnavailable,
				"local node unavailable",
				raft.Status{},
			)
			return
		}

		if status.Role != raft.Leader {
			writeRoutingError(
				w,
				http.StatusServiceUnavailable,
				routingCodeNotLeader,
				"forwarded request reached a non-leader",
				status,
			)
			return
		}

		if router.verifyLeadership {
			ctx, cancel := context.WithTimeout(
				r.Context(),
				routingAttemptTimeout,
			)

			target, err := router.discover(ctx)
			cancel()

			if err != nil {
				writeRoutingError(
					w,
					http.StatusServiceUnavailable,
					routingCodeUnavailable,
					"leadership could not be verified",
					status,
				)
				return
			}

			if !target.Local {
				writeRoutingError(
					w,
					http.StatusServiceUnavailable,
					routingCodeNotLeader,
					"leadership changed",
					status,
				)
				return
			}
		}

		router.local.ServeHTTP(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		routingTimeout,
	)
	defer cancel()

	bodyReader := http.MaxBytesReader(
		w,
		r.Body,
		routingMaxBody,
	)
	defer bodyReader.Close()

	body, err := io.ReadAll(bodyReader)
	if err != nil {
		http.Error(
			w,
			"invalid or oversized request body",
			http.StatusBadRequest,
		)
		return
	}

	router.executeWithRetry(ctx, w, r, body)
}

func (router *requestRouter) executeAttempt(
	ctx context.Context,
	original *http.Request,
	body []byte,
) routingAttemptResult {
	status, err := router.status()

	if err != nil {
		return newRoutingFailure(
			http.StatusServiceUnavailable,
			routingCodeUnavailable,
			"local node unavailable",
			raft.Status{},
		)
	}

	if status.Role == raft.Leader &&
		!router.verifyLeadership {
		return router.executeLocal(
			ctx, original, body,
		)
	}

	target, err := router.discover(ctx)

	if err != nil {
		return newRoutingFailure(
			http.StatusServiceUnavailable,
			routingCodeUnavailable,
			"leader unavailable",
			status,
		)
	}

	if target.Local {
		return router.executeLocal(
			ctx, original, body,
		)
	}

	if target.Address == "" {
		return newRoutingFailure(
			http.StatusServiceUnavailable,
			routingCodeUnavailable,
			"leader address unavailable",
			status,
		)
	}

	return router.forward(
		ctx, original, target, body,
	)
}

func (router *requestRouter) executeLocal(
	ctx context.Context,
	original *http.Request,
	body []byte,
) routingAttemptResult {
	req := original.Clone(ctx)

	req.Body = io.NopCloser(
		bytes.NewReader(body),
	)

	req.ContentLength = int64(len(body))

	rec := newRoutingResponseBuffer()

	router.local.ServeHTTP(rec, req)

	if rec.overflow || rec.status == 0 {
		return routingTransportFailure(
			original.Method,
		)
	}

	return routingAttemptResult{
		status:      rec.status,
		contentType: rec.header.Get("Content-Type"),
		body:        rec.body.Bytes(),
	}
}

func (router *requestRouter) forward(
	ctx context.Context,
	original *http.Request,
	target leaderTarget,
	body []byte,
) routingAttemptResult {
	targetURL := &url.URL{
		Scheme:   "http",
		Host:     target.Address,
		Path:     original.URL.Path,
		RawPath:  original.URL.RawPath,
		RawQuery: original.URL.RawQuery,
	}

	req, err := http.NewRequestWithContext(
		ctx,
		original.Method,
		targetURL.String(),
		bytes.NewReader(body),
	)

	if err != nil {
		return newRoutingFailure(
			http.StatusServiceUnavailable,
			routingCodeUnavailable,
			"cannot construct forwarded request",
			raft.Status{},
		)
	}

	if contentType := original.Header.Get(
		"Content-Type",
	); contentType != "" {
		req.Header.Set(
			"Content-Type",
			contentType,
		)
	}

	req.Header.Set(routingForwardHeader, "1")

	resp, err := router.client.Do(req)

	if err != nil {
		return routingTransportFailure(
			original.Method,
		)
	}

	defer resp.Body.Close()

	responseBody, err := io.ReadAll(
		io.LimitReader(
			resp.Body,
			routingMaxResponse+1,
		),
	)

	if err != nil ||
		len(responseBody) > routingMaxResponse {
		return routingTransportFailure(
			original.Method,
		)
	}

	if resp.StatusCode >= 300 &&
		resp.StatusCode < 400 {
		return routingTransportFailure(
			original.Method,
		)
	}

	return routingAttemptResult{
		status:      resp.StatusCode,
		contentType: resp.Header.Get("Content-Type"),
		body:        responseBody,
	}
}
