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

type gatewayDeleteMock struct {
	gatewayKVClient

	result client.MutationResult
	err    error

	calls       int
	key         string
	identity    client.MutationIdentity
	hasDeadline bool
}

func (m *gatewayDeleteMock) DeleteWithIdentity(
	ctx context.Context,
	key string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	m.calls++
	m.key = key
	m.identity = identity
	_, m.hasDeadline = ctx.Deadline()

	return m.result, m.err
}

func gatewayDeleteRequest(
	mock *gatewayDeleteMock,
	path string,
	body string,
) *httptest.ResponseRecorder {
	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodDelete,
		path,
		strings.NewReader(body),
	)

	request.Header.Set("Content-Type", "application/json")

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	return response
}

func TestGatewayDeleteSuccess(t *testing.T) {
	mock := &gatewayDeleteMock{
		result: client.MutationResult{
			Applied: true,
		},
	}

	response := gatewayDeleteRequest(
		mock,
		"/kv/alpha",
		`{"client_id":"client-1","request_id":"request-1"}`,
	)

	if response.Code != http.StatusOK {
		t.Fatalf("status: %d, body: %s",
			response.Code, response.Body.String())
	}

	if mock.calls != 1 || mock.key != "alpha" {
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
		result.ClientID != "client-1" ||
		result.RequestID != "request-1" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestGatewayDeleteMissingKey(t *testing.T) {
	mock := &gatewayDeleteMock{
		result: client.MutationResult{
			Applied: false,
		},
	}

	response := gatewayDeleteRequest(
		mock,
		"/kv/missing",
		`{"client_id":"c1","request_id":"r1"}`,
	)

	if response.Code != http.StatusOK {
		t.Fatalf("status: %d", response.Code)
	}

	var result gatewayMutationResponse

	if err := json.Unmarshal(
		response.Body.Bytes(), &result,
	); err != nil {
		t.Fatal(err)
	}

	if result.Applied {
		t.Fatal("missing key should not report applied")
	}

	if mock.calls != 1 {
		t.Fatal("expected one backend invocation")
	}
}

func TestGatewayDeleteEncodedKeys(t *testing.T) {
	cases := []struct {
		path string
		key  string
	}{
		{"/kv/a%2Fb", "a/b"},
		{"/kv/hello%20world", "hello world"},
		{"/kv/%E4%BD%A0%E5%A5%BD", "你好"},
		{"/kv/a%3Fb", "a?b"},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			mock := &gatewayDeleteMock{
				result: client.MutationResult{
					Applied: true,
				},
			}

			response := gatewayDeleteRequest(
				mock,
				tc.path,
				`{"client_id":"c1","request_id":"r1"}`,
			)

			if response.Code != http.StatusOK {
				t.Fatalf("status: %d", response.Code)
			}

			if mock.key != tc.key {
				t.Fatalf("got %q, want %q",
					mock.key, tc.key)
			}
		})
	}
}

func TestGatewayDeleteInvalidKey(t *testing.T) {
	mock := &gatewayDeleteMock{}

	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/kv/alpha",
		strings.NewReader(
			`{"client_id":"c1","request_id":"r1"}`,
		),
	)

	request.SetPathValue("key", string([]byte{0xff}))

	response := httptest.NewRecorder()
	handler.handleDelete(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status: %d", response.Code)
	}

	if mock.calls != 0 {
		t.Fatal("invalid key reached backend")
	}
}

func TestGatewayDeleteInvalidBodies(t *testing.T) {
	cases := map[string]string{
		"Empty":              "",
		"Null":               `null`,
		"Array":              `[]`,
		"Malformed":          `{"client_id":`,
		"MissingClientID":    `{"request_id":"r1"}`,
		"MissingRequestID":   `{"client_id":"c1"}`,
		"EmptyIdentity":      `{"client_id":"","request_id":""}`,
		"UnexpectedValue":    `{"client_id":"c1","request_id":"r1","value":"100"}`,
		"UnexpectedExpected": `{"client_id":"c1","request_id":"r1","expected":"100"}`,
		"UnknownField":       `{"client_id":"c1","request_id":"r1","extra":true}`,
		"MultipleObjects":    `{"client_id":"c1","request_id":"r1"} {}`,
		"WrongIdentityType":  `{"client_id":100,"request_id":"r1"}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			mock := &gatewayDeleteMock{}

			response := gatewayDeleteRequest(
				mock, "/kv/alpha", body,
			)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("status: %d", response.Code)
			}

			if mock.calls != 0 {
				t.Fatal("invalid request reached backend")
			}
		})
	}
}

func TestGatewayDeleteOversizedBody(t *testing.T) {
	mock := &gatewayDeleteMock{}

	body := `{"client_id":"` +
		strings.Repeat("x", gatewayMaxRequestSize) +
		`","request_id":"r1"}`

	response := gatewayDeleteRequest(
		mock, "/kv/alpha", body,
	)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status: %d", response.Code)
	}

	if mock.calls != 0 {
		t.Fatal("oversized request reached backend")
	}
}

func TestGatewayDeleteBackendErrors(t *testing.T) {
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
			"InvalidIdentity",
			client.ErrInvalidMutationIdentity,
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
			"WrongShard",
			&client.RoutingFailure{
				Detail: &shardraftpb.RoutingError{
					Code: shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
				},
			},
			http.StatusServiceUnavailable,
			"leader_unavailable",
		},
		{
			"InvalidRequest",
			&client.RoutingFailure{
				Detail: &shardraftpb.RoutingError{
					Code: shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
				},
			},
			http.StatusBadRequest,
			"invalid_request",
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
			mock := &gatewayDeleteMock{
				err: tc.err,
			}

			response := gatewayDeleteRequest(
				mock,
				"/kv/alpha",
				`{"client_id":"c1","request_id":"r1"}`,
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

func TestGatewayDeleteIdentityReplay(t *testing.T) {
	mock := &gatewayDeleteMock{
		result: client.MutationResult{
			Applied: true,
		},
	}

	body := `{"client_id":"stable-client","request_id":"stable-delete"}`

	for i := 0; i < 10; i++ {
		response := gatewayDeleteRequest(
			mock, "/kv/alpha", body,
		)

		if response.Code != http.StatusOK {
			t.Fatalf("status: %d", response.Code)
		}

		if mock.identity.ClientID != "stable-client" ||
			mock.identity.RequestID != "stable-delete" {
			t.Fatal("request identity changed")
		}
	}

	if mock.calls != 10 {
		t.Fatalf("unexpected calls: %d", mock.calls)
	}
}
