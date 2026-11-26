package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/client"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

type gatewayPutMock struct {
	gatewayKVClient

	result client.MutationResult
	err    error

	calls       int
	key         string
	value       string
	identity    client.MutationIdentity
	hasDeadline bool
}

func (m *gatewayPutMock) PutWithIdentity(
	ctx context.Context,
	key string,
	value string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	m.calls++
	m.key = key
	m.value = value
	m.identity = identity
	_, m.hasDeadline = ctx.Deadline()

	return m.result, m.err
}

func gatewayPutRequest(
	mock *gatewayPutMock,
	path string,
	body string,
) *httptest.ResponseRecorder {
	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodPut,
		path,
		strings.NewReader(body),
	)

	request.Header.Set("Content-Type", "application/json")

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	return response
}

func TestGatewayPutSuccess(t *testing.T) {
	mock := &gatewayPutMock{
		result: client.MutationResult{
			Applied: true,
			Value:   "100",
		},
	}

	response := gatewayPutRequest(
		mock,
		"/kv/alpha",
		`{"value":"100","client_id":"client-1","request_id":"request-1"}`,
	)

	if response.Code != http.StatusOK {
		t.Fatalf("status: %d, body: %s",
			response.Code, response.Body.String())
	}

	if mock.calls != 1 ||
		mock.key != "alpha" ||
		mock.value != "100" {
		t.Fatal("unexpected backend invocation")
	}

	if mock.identity.ClientID != "client-1" ||
		mock.identity.RequestID != "request-1" {
		t.Fatal("mutation identity changed")
	}

	if !mock.hasDeadline {
		t.Fatal("expected request deadline")
	}

	var result gatewayMutationResponse

	if err := json.Unmarshal(
		response.Body.Bytes(), &result,
	); err != nil {
		t.Fatal(err)
	}

	if !result.Applied ||
		result.Value != "100" ||
		result.ClientID != "client-1" ||
		result.RequestID != "request-1" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestGatewayPutEmptyValue(t *testing.T) {
	mock := &gatewayPutMock{
		result: client.MutationResult{Applied: true},
	}

	response := gatewayPutRequest(
		mock,
		"/kv/empty",
		`{"value":"","client_id":"c1","request_id":"r1"}`,
	)

	if response.Code != http.StatusOK ||
		mock.calls != 1 ||
		mock.value != "" {
		t.Fatal("empty value should be accepted")
	}
}

func TestGatewayPutEncodedKey(t *testing.T) {
	mock := &gatewayPutMock{
		result: client.MutationResult{Applied: true},
	}

	response := gatewayPutRequest(
		mock,
		"/kv/a%2Fb",
		`{"value":"hello","client_id":"c1","request_id":"r1"}`,
	)

	if response.Code != http.StatusOK ||
		mock.key != "a/b" {
		t.Fatal("encoded key was not preserved")
	}
}

func TestGatewayPutInvalidBodies(t *testing.T) {
	cases := map[string]string{
		"Empty":              "",
		"Null":               `null`,
		"Array":              `[]`,
		"Malformed":          `{"value":`,
		"MissingValue":       `{"client_id":"c1","request_id":"r1"}`,
		"NullValue":          `{"value":null,"client_id":"c1","request_id":"r1"}`,
		"MissingClientID":    `{"value":"100","request_id":"r1"}`,
		"MissingRequestID":   `{"value":"100","client_id":"c1"}`,
		"EmptyIdentity":      `{"value":"100","client_id":"","request_id":""}`,
		"UnknownField":       `{"value":"100","client_id":"c1","request_id":"r1","extra":true}`,
		"UnexpectedExpected": `{"value":"100","expected":"50","client_id":"c1","request_id":"r1"}`,
		"WrongValueType":     `{"value":100,"client_id":"c1","request_id":"r1"}`,
		"MultipleObjects":    `{"value":"100","client_id":"c1","request_id":"r1"} {}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			mock := &gatewayPutMock{}

			response := gatewayPutRequest(
				mock, "/kv/alpha", body,
			)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("unexpected status: %d", response.Code)
			}

			if mock.calls != 0 {
				t.Fatal("invalid request reached backend")
			}
		})
	}
}

func TestGatewayPutOversizedBody(t *testing.T) {
	mock := &gatewayPutMock{}

	body := `{"value":"` +
		strings.Repeat("x", gatewayMaxRequestSize) +
		`","client_id":"c1","request_id":"r1"}`

	response := gatewayPutRequest(
		mock, "/kv/alpha", body,
	)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status: %d", response.Code)
	}

	if mock.calls != 0 {
		t.Fatal("oversized request reached backend")
	}
}

func TestGatewayPutBackendErrors(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{
			"UnknownOutcome",
			client.ErrMutationOutcomeUnknown,
			http.StatusGatewayTimeout,
			"outcome_unknown",
		},
		{
			"UnknownOutcomePriority",
			errors.Join(
				client.ErrMutationOutcomeUnknown,
				context.DeadlineExceeded,
			),
			http.StatusGatewayTimeout,
			"outcome_unknown",
		},
		{
			"InvalidValue",
			client.ErrInvalidValue,
			http.StatusBadRequest,
			"invalid_request",
		},
		{
			"Deadline",
			context.DeadlineExceeded,
			http.StatusGatewayTimeout,
			"deadline_exceeded",
		},
		{
			"Canceled",
			context.Canceled,
			http.StatusRequestTimeout,
			"request_canceled",
		},
		{
			"Unavailable",
			client.ErrRetryExhausted,
			http.StatusServiceUnavailable,
			"leader_unavailable",
		},
		{
			"Conflict",
			&client.RoutingFailure{
				Detail: &shardraftpb.RoutingError{
					Code: shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT,
				},
			},
			http.StatusConflict,
			"request_conflict",
		},
		{
			"NotLeader",
			&client.RoutingFailure{
				Detail: &shardraftpb.RoutingError{
					Code: shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
				},
			},
			http.StatusServiceUnavailable,
			"leader_unavailable",
		},
		{
			"Internal",
			errors.New("private backend error"),
			http.StatusInternalServerError,
			"internal_error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &gatewayPutMock{err: tc.err}

			response := gatewayPutRequest(
				mock,
				"/kv/alpha",
				`{"value":"100","client_id":"c1","request_id":"r1"}`,
			)

			if response.Code != tc.status {
				t.Fatalf("got %d, want %d",
					response.Code, tc.status)
			}

			var result gatewayMutationErrorResponse

			if err := json.Unmarshal(
				response.Body.Bytes(), &result,
			); err != nil {
				t.Fatal(err)
			}

			if result.Code != tc.code ||
				result.ClientID != "c1" ||
				result.RequestID != "r1" {
				t.Fatalf("unexpected error: %+v", result)
			}

			if mock.calls != 1 {
				t.Fatal("gateway must delegate retries to client")
			}
		})
	}
}

func TestGatewayPutIdentityReplay(t *testing.T) {
	mock := &gatewayPutMock{
		result: client.MutationResult{Applied: true},
	}

	body := `{"value":"100","client_id":"stable-client","request_id":"stable-request"}`

	for i := 0; i < 10; i++ {
		response := gatewayPutRequest(
			mock, "/kv/alpha", body,
		)

		if response.Code != http.StatusOK {
			t.Fatalf("status: %d", response.Code)
		}

		if mock.identity.ClientID != "stable-client" ||
			mock.identity.RequestID != "stable-request" {
			t.Fatal("request identity changed")
		}
	}

	if mock.calls != 10 {
		t.Fatalf("unexpected calls: %d", mock.calls)
	}
}
