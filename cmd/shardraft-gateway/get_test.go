package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/client"
)

type gatewayReadMock struct {
	gatewayKVClient

	value string
	found bool
	err   error

	key               string
	linearizableCalls int
	localCalls        int
	hasDeadline       bool
}

func (m *gatewayReadMock) Get(
	ctx context.Context,
	key string,
) (string, bool, error) {
	m.key = key
	m.linearizableCalls++
	_, m.hasDeadline = ctx.Deadline()

	return m.value, m.found, m.err
}

func (m *gatewayReadMock) GetLocal(
	ctx context.Context,
	key string,
) (string, bool, error) {
	m.key = key
	m.localCalls++
	_, m.hasDeadline = ctx.Deadline()

	return m.value, m.found, m.err
}

func TestGatewayGetLinearizable(t *testing.T) {
	mock := &gatewayReadMock{
		value: "100",
		found: true,
	}

	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodGet,
		"/kv/alpha",
		nil,
	)

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", response.Code)
	}

	var result gatewayGetResponse

	if err := json.Unmarshal(
		response.Body.Bytes(), &result,
	); err != nil {
		t.Fatal(err)
	}

	if result.Key != "alpha" ||
		result.Value != "100" ||
		!result.Found {
		t.Fatalf("unexpected result: %+v", result)
	}

	if mock.linearizableCalls != 1 ||
		mock.localCalls != 0 {
		t.Fatal("expected one linearizable read")
	}

	if !mock.hasDeadline {
		t.Fatal("expected request deadline")
	}

	if response.Header().Get("Content-Type") !=
		"application/json" {
		t.Fatal("expected JSON content type")
	}
}

func TestGatewayGetReadModes(t *testing.T) {
	cases := []struct {
		mode  string
		local bool
	}{
		{"", false},
		{"linearizable", false},
		{"local", true},
	}

	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			mock := &gatewayReadMock{
				value: "100",
				found: true,
			}

			handler := newGatewayHandler(
				mock, time.Second,
			)

			request := httptest.NewRequest(
				http.MethodGet,
				"/kv/alpha?read_mode="+tc.mode,
				nil,
			)

			response := httptest.NewRecorder()
			handler.routes().ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status: %d", response.Code)
			}

			if tc.local {
				if mock.localCalls != 1 ||
					mock.linearizableCalls != 0 {
					t.Fatal("expected local read")
				}
			} else {
				if mock.linearizableCalls != 1 ||
					mock.localCalls != 0 {
					t.Fatal("expected linearizable read")
				}
			}
		})
	}
}

func TestGatewayGetInvalidMode(t *testing.T) {
	mock := &gatewayReadMock{}

	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodGet,
		"/kv/alpha?read_mode=invalid",
		nil,
	)

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status: %d", response.Code)
	}

	if mock.linearizableCalls != 0 ||
		mock.localCalls != 0 {
		t.Fatal("invalid request reached backend")
	}
}

func TestGatewayGetMissingKey(t *testing.T) {
	mock := &gatewayReadMock{found: false}

	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodGet,
		"/kv/missing",
		nil,
	)

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status: %d", response.Code)
	}

	var result gatewayErrorResponse

	if err := json.Unmarshal(
		response.Body.Bytes(), &result,
	); err != nil {
		t.Fatal(err)
	}

	if result.Code != "key_not_found" {
		t.Fatalf("unexpected error: %+v", result)
	}
}

func TestGatewayGetEmptyValue(t *testing.T) {
	mock := &gatewayReadMock{
		value: "",
		found: true,
	}

	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodGet,
		"/kv/empty-value",
		nil,
	)

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status: %d", response.Code)
	}
}

func TestGatewayGetEncodedKeys(t *testing.T) {
	cases := []struct {
		path string
		key  string
	}{
		{"/kv/hello%20world", "hello world"},
		{"/kv/a%2Fb", "a/b"},
		{"/kv/%E4%BD%A0%E5%A5%BD", "你好"},
		{"/kv/a%3Fb", "a?b"},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			mock := &gatewayReadMock{
				value: "value",
				found: true,
			}

			handler := newGatewayHandler(
				mock, time.Second,
			)

			request := httptest.NewRequest(
				http.MethodGet, tc.path, nil,
			)

			response := httptest.NewRecorder()
			handler.routes().ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status: %d", response.Code)
			}

			if mock.key != tc.key {
				t.Fatalf(
					"got %q, want %q",
					mock.key, tc.key,
				)
			}
		})
	}
}

func TestGatewayGetBackendErrors(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{
			"InvalidKey",
			client.ErrInvalidKey,
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
			"RetryExhausted",
			client.ErrRetryExhausted,
			http.StatusServiceUnavailable,
			"leader_unavailable",
		},
		{
			"MetadataUnavailable",
			client.ErrMetadataUnavailable,
			http.StatusServiceUnavailable,
			"leader_unavailable",
		},
		{
			"MetadataRefresh",
			client.ErrMetadataRefresh,
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
			mock := &gatewayReadMock{err: tc.err}

			handler := newGatewayHandler(
				mock, time.Second,
			)

			request := httptest.NewRequest(
				http.MethodGet, "/kv/alpha", nil,
			)

			response := httptest.NewRecorder()
			handler.routes().ServeHTTP(response, request)

			if response.Code != tc.status {
				t.Fatalf(
					"got status %d, want %d",
					response.Code, tc.status,
				)
			}

			var result gatewayErrorResponse

			if err := json.Unmarshal(
				response.Body.Bytes(), &result,
			); err != nil {
				t.Fatal(err)
			}

			if result.Code != tc.code {
				t.Fatalf("unexpected error: %+v", result)
			}
		})
	}
}

func TestGatewayGetUnsupportedMethod(t *testing.T) {
	mock := &gatewayReadMock{}

	handler := newGatewayHandler(mock, time.Second)

	request := httptest.NewRequest(
		http.MethodPatch,
		"/kv/alpha",
		nil,
	)

	response := httptest.NewRecorder()
	handler.routes().ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: %d", response.Code)
	}
}
