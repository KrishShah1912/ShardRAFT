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

type gatewayCASMock struct {
	gatewayKVClient

	result client.MutationResult
	err    error

	calls       int
	key         string
	expected    string
	value       string
	identity    client.MutationIdentity
	hasDeadline bool
}

func (m *gatewayCASMock) CompareAndSwapWithIdentity(
	ctx context.Context,
	key string,
	expected string,
	value string,
	identity client.MutationIdentity,
) (client.MutationResult, error) {
	m.calls++
	m.key = key
	m.expected = expected
	m.value = value
	m.identity = identity
	_, m.hasDeadline = ctx.Deadline()

	return m.result, m.err
}

func gatewayCASRequest(
	mock *gatewayCASMock,
	path string,
	body string,
) *httptest.ResponseRecorder {
	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodPost,
		path,
		strings.NewReader(body),
	)

	request.Header.Set("Content-Type", "application/json")

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	return response
}

func TestGatewayCASSuccess(t *testing.T) {
	mock := &gatewayCASMock{
		result: client.MutationResult{
			Applied: true,
			Value:   "200",
		},
	}

	response := gatewayCASRequest(
		mock,
		"/kv/alpha/cas",
		`{"expected":"100","value":"200","client_id":"c1","request_id":"r1"}`,
	)

	if response.Code != http.StatusOK {
		t.Fatalf("status: %d, body: %s",
			response.Code, response.Body.String())
	}

	if mock.calls != 1 ||
		mock.key != "alpha" ||
		mock.expected != "100" ||
		mock.value != "200" {
		t.Fatal("unexpected backend invocation")
	}

	if mock.identity.ClientID != "c1" ||
		mock.identity.RequestID != "r1" {
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
		result.Value != "200" ||
		result.ClientID != "c1" ||
		result.RequestID != "r1" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestGatewayCASMismatch(t *testing.T) {
	mock := &gatewayCASMock{
		result: client.MutationResult{
			Applied: false,
			Value:   "300",
		},
	}

	response := gatewayCASRequest(
		mock,
		"/kv/alpha/cas",
		`{"expected":"100","value":"200","client_id":"c1","request_id":"r1"}`,
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

	if result.Applied || result.Value != "300" {
		t.Fatalf("unexpected result: %+v", result)
	}

	if mock.calls != 1 {
		t.Fatal("CAS mismatch must not trigger gateway retries")
	}
}

func TestGatewayCASEmptyValues(t *testing.T) {
	cases := []struct {
		name     string
		expected string
		value    string
	}{
		{"EmptyExpected", "", "200"},
		{"EmptyValue", "100", ""},
		{"BothEmpty", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &gatewayCASMock{
				result: client.MutationResult{
					Applied: true,
				},
			}

			body, err := json.Marshal(map[string]string{
				"expected":   tc.expected,
				"value":      tc.value,
				"client_id":  "c1",
				"request_id": "r1",
			})

			if err != nil {
				t.Fatal(err)
			}

			response := gatewayCASRequest(
				mock, "/kv/alpha/cas", string(body),
			)

			if response.Code != http.StatusOK {
				t.Fatalf("status: %d", response.Code)
			}

			if mock.calls != 1 ||
				mock.expected != tc.expected ||
				mock.value != tc.value {
				t.Fatal("empty values were not preserved")
			}
		})
	}
}

func TestGatewayCASEncodedKeys(t *testing.T) {
	cases := []struct {
		path string
		key  string
	}{
		{"/kv/a%2Fb/cas", "a/b"},
		{"/kv/hello%20world/cas", "hello world"},
		{"/kv/%E4%BD%A0%E5%A5%BD/cas", "你好"},
		{"/kv/a%3Fb/cas", "a?b"},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			mock := &gatewayCASMock{
				result: client.MutationResult{
					Applied: true,
				},
			}

			response := gatewayCASRequest(
				mock,
				tc.path,
				`{"expected":"100","value":"200","client_id":"c1","request_id":"r1"}`,
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

func TestGatewayCASInvalidKey(t *testing.T) {
	for _, key := range []string{"", string([]byte{0xff})} {
		mock := &gatewayCASMock{}

		handler := newGatewayHandler(mock, time.Second)

		request := httptest.NewRequest(
			http.MethodPost,
			"/kv/alpha/cas",
			strings.NewReader(
				`{"expected":"100","value":"200","client_id":"c1","request_id":"r1"}`,
			),
		)

		request.SetPathValue("key", key)

		response := httptest.NewRecorder()
		handler.handleCAS(response, request)

		if response.Code != http.StatusBadRequest {
			t.Fatalf("status: %d", response.Code)
		}

		if mock.calls != 0 {
			t.Fatal("invalid key reached backend")
		}
	}
}

func TestGatewayCASInvalidBodies(t *testing.T) {
	cases := map[string]string{
		"Empty":             "",
		"Null":              `null`,
		"Array":             `[]`,
		"Malformed":         `{"expected":`,
		"MissingExpected":   `{"value":"200","client_id":"c1","request_id":"r1"}`,
		"MissingValue":      `{"expected":"100","client_id":"c1","request_id":"r1"}`,
		"NullExpected":      `{"expected":null,"value":"200","client_id":"c1","request_id":"r1"}`,
		"NullValue":         `{"expected":"100","value":null,"client_id":"c1","request_id":"r1"}`,
		"MissingClientID":   `{"expected":"100","value":"200","request_id":"r1"}`,
		"MissingRequestID":  `{"expected":"100","value":"200","client_id":"c1"}`,
		"EmptyIdentity":     `{"expected":"100","value":"200","client_id":"","request_id":""}`,
		"WrongExpectedType": `{"expected":100,"value":"200","client_id":"c1","request_id":"r1"}`,
		"WrongValueType":    `{"expected":"100","value":200,"client_id":"c1","request_id":"r1"}`,
		"UnknownField":      `{"expected":"100","value":"200","client_id":"c1","request_id":"r1","extra":true}`,
		"MultipleObjects":   `{"expected":"100","value":"200","client_id":"c1","request_id":"r1"} {}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			mock := &gatewayCASMock{}

			response := gatewayCASRequest(
				mock, "/kv/alpha/cas", body,
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

func TestGatewayCASOversizedBody(t *testing.T) {
	mock := &gatewayCASMock{}

	body := `{"expected":"100","value":"` +
		strings.Repeat("x", gatewayMaxRequestSize) +
		`","client_id":"c1","request_id":"r1"}`

	response := gatewayCASRequest(
		mock, "/kv/alpha/cas", body,
	)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status: %d", response.Code)
	}

	if mock.calls != 0 {
		t.Fatal("oversized request reached backend")
	}
}

func TestGatewayCASBackendErrors(t *testing.T) {
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
			"LeaderUnavailable",
			&client.RoutingFailure{
				Detail: &shardraftpb.RoutingError{
					Code: shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
				},
			},
			http.StatusServiceUnavailable,
			"leader_unavailable",
		},
		{
			"RoutingOutcomeUnknown",
			&client.RoutingFailure{
				Detail: &shardraftpb.RoutingError{
					Code: shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN,
				},
			},
			http.StatusGatewayTimeout,
			"outcome_unknown",
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
			mock := &gatewayCASMock{err: tc.err}

			response := gatewayCASRequest(
				mock,
				"/kv/alpha/cas",
				`{"expected":"100","value":"200","client_id":"c1","request_id":"r1"}`,
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

func TestGatewayCASIdentityReplay(t *testing.T) {
	mock := &gatewayCASMock{
		result: client.MutationResult{
			Applied: true,
			Value:   "200",
		},
	}

	body := `{"expected":"100","value":"200","client_id":"stable-client","request_id":"stable-cas"}`

	for i := 0; i < 10; i++ {
		response := gatewayCASRequest(
			mock, "/kv/alpha/cas", body,
		)

		if response.Code != http.StatusOK {
			t.Fatalf("status: %d", response.Code)
		}

		if mock.identity.ClientID != "stable-client" ||
			mock.identity.RequestID != "stable-cas" ||
			mock.expected != "100" ||
			mock.value != "200" {
			t.Fatal("mutation identity or payload changed")
		}
	}

	if mock.calls != 10 {
		t.Fatalf("unexpected calls: %d", mock.calls)
	}
}

func TestGatewayCASUnsupportedMethod(t *testing.T) {
	mock := &gatewayCASMock{}

	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodPut,
		"/kv/alpha/cas",
		nil,
	)

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: %d", response.Code)
	}

	if mock.calls != 0 {
		t.Fatal("unsupported method reached backend")
	}
}
