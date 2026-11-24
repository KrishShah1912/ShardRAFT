package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

const (
	routingMaxAttempts = 3
	routingRetryDelay  = 100 * time.Millisecond
)

type routingAttemptResult struct {
	status      int
	contentType string
	body        []byte
}

func newRoutingFailure(
	status int,
	code string,
	message string,
	raftStatus raft.Status,
) routingAttemptResult {
	body, _ := json.Marshal(routingErrorResponse{
		Error:    message,
		Code:     code,
		LeaderID: raftStatus.LeaderID,
		Term:     raftStatus.Term,
	})

	body = append(body, '\n')

	return routingAttemptResult{
		status:      status,
		contentType: "application/json",
		body:        body,
	}
}

func (result routingAttemptResult) routingCode() string {
	var response routingErrorResponse

	if err := json.Unmarshal(
		result.body,
		&response,
	); err != nil {
		return ""
	}

	return response.Code
}

func (result routingAttemptResult) retryable() bool {
	code := result.routingCode()

	switch result.status {
	case http.StatusServiceUnavailable:
		return code == routingCodeNotLeader ||
			code == routingCodeUnavailable

	case http.StatusGatewayTimeout:
		return code == routingCodeUnknown

	default:
		return false
	}
}

func (result routingAttemptResult) unknownOutcome() bool {
	if result.status == http.StatusGatewayTimeout {
		return true
	}

	if result.status >= 500 {
		code := result.routingCode()

		return code != routingCodeNotLeader &&
			code != routingCodeUnavailable
	}

	return false
}

func (result routingAttemptResult) write(
	w http.ResponseWriter,
) {
	if result.contentType != "" {
		w.Header().Set(
			"Content-Type",
			result.contentType,
		)
	}

	w.WriteHeader(result.status)
	_, _ = w.Write(result.body)
}

func routingTransportFailure(
	method string,
) routingAttemptResult {
	if method == http.MethodGet {
		return newRoutingFailure(
			http.StatusServiceUnavailable,
			routingCodeUnavailable,
			"leader request failed",
			raft.Status{},
		)
	}

	return newRoutingFailure(
		http.StatusGatewayTimeout,
		routingCodeUnknown,
		"mutation outcome unknown",
		raft.Status{},
	)
}

func hasRoutingRequestIdentity(
	method string,
	body []byte,
) bool {
	if method == http.MethodGet {
		return true
	}

	var identity struct {
		ClientID  string `json:"client_id"`
		RequestID string `json:"request_id"`
	}

	if err := json.Unmarshal(
		body,
		&identity,
	); err != nil {
		return false
	}

	return identity.ClientID != "" &&
		identity.RequestID != ""
}

func waitForRoutingRetry(
	ctx context.Context,
	attempt int,
) error {
	delay := routingRetryDelay *
		time.Duration(1<<uint(attempt))

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-timer.C:
		return nil
	}
}

func (router *requestRouter) executeWithRetry(
	ctx context.Context,
	w http.ResponseWriter,
	original *http.Request,
	body []byte,
) {
	mutation := original.Method != http.MethodGet

	canRetry := hasRoutingRequestIdentity(
		original.Method,
		body,
	)

	unknown := false

	last := newRoutingFailure(
		http.StatusServiceUnavailable,
		routingCodeUnavailable,
		"leader unavailable",
		raft.Status{},
	)

	for attempt := 0; attempt < routingMaxAttempts; attempt++ {
		if ctx.Err() != nil {
			break
		}

		attemptCtx, cancel := context.WithTimeout(
			ctx,
			routingAttemptTimeout,
		)

		last = router.executeAttempt(
			attemptCtx,
			original,
			body,
		)

		cancel()

		if mutation && last.unknownOutcome() {
			unknown = true
		}

		// A successful response resolves an earlier
		// unknown outcome for the same request identity.
		if last.status == http.StatusOK {
			last.write(w)
			return
		}

		if !last.retryable() || !canRetry {
			break
		}

		if attempt+1 >= routingMaxAttempts {
			break
		}

		if err := waitForRoutingRetry(
			ctx,
			attempt,
		); err != nil {
			break
		}
	}

	// Never replace an ambiguous mutation result with
	// a later leader-unavailable response.
	if mutation && unknown {
		last = newRoutingFailure(
			http.StatusGatewayTimeout,
			routingCodeUnknown,
			"commit not confirmed; request outcome unknown",
			raft.Status{},
		)
	}

	last.write(w)
}

// Buffer only the bounded KV response, not streaming
// or administrative endpoints.
type routingResponseBuffer struct {
	header   http.Header
	body     bytes.Buffer
	status   int
	overflow bool
}

func newRoutingResponseBuffer() *routingResponseBuffer {
	return &routingResponseBuffer{
		header: make(http.Header),
	}
}

func (rec *routingResponseBuffer) Header() http.Header {
	return rec.header
}

func (rec *routingResponseBuffer) WriteHeader(status int) {
	if rec.status == 0 {
		rec.status = status
	}
}

func (rec *routingResponseBuffer) Write(
	data []byte,
) (int, error) {
	if rec.status == 0 {
		rec.status = http.StatusOK
	}

	if rec.overflow ||
		len(data) > routingMaxResponse-rec.body.Len() {
		rec.overflow = true
		return 0, errors.New(
			"routing response exceeds size limit",
		)
	}

	return rec.body.Write(data)
}
