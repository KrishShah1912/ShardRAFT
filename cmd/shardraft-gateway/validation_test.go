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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGatewayValidationGetInvalidKey(t *testing.T) {
	for _, key := range []string{"", string([]byte{0xff})} {
		mock := &gatewayReadMock{}
		handler := newGatewayHandler(mock, time.Second)

		request := httptest.NewRequest(
			http.MethodGet, "/kv/alpha", nil,
		)
		request.SetPathValue("key", key)

		response := httptest.NewRecorder()
		handler.handleGet(response, request)

		if response.Code != http.StatusBadRequest {
			t.Fatalf("status: %d", response.Code)
		}

		if mock.linearizableCalls != 0 ||
			mock.localCalls != 0 {
			t.Fatal("invalid key reached backend")
		}
	}
}

func TestGatewayValidationPutInvalidKey(t *testing.T) {
	for _, key := range []string{"", string([]byte{0xff})} {
		mock := &gatewayPutMock{}
		handler := newGatewayHandler(mock, time.Second)

		request := httptest.NewRequest(
			http.MethodPut,
			"/kv/alpha",
			strings.NewReader(
				`{"value":"100","client_id":"c1","request_id":"r1"}`,
			),
		)

		request.SetPathValue("key", key)

		response := httptest.NewRecorder()
		handler.handlePut(response, request)

		if response.Code != http.StatusBadRequest {
			t.Fatalf("status: %d", response.Code)
		}

		if mock.calls != 0 {
			t.Fatal("invalid key reached backend")
		}
	}
}

func TestGatewayValidationStrictJSON(t *testing.T) {
	cases := map[string]string{
		"DuplicateValue":     `{"value":"100","value":"200","client_id":"c1","request_id":"r1"}`,
		"DuplicateClientID":  `{"value":"100","client_id":"c1","client_id":"c2","request_id":"r1"}`,
		"DuplicateRequestID": `{"value":"100","client_id":"c1","request_id":"r1","request_id":"r2"}`,
		"EscapedDuplicate":   `{"value":"100","\u0076alue":"200","client_id":"c1","request_id":"r1"}`,
		"InvalidUTF8": `{"value":"` + string([]byte{0xff}) +
			`","client_id":"c1","request_id":"r1"}`,
		"NullExpected":        `{"value":"100","expected":null,"client_id":"c1","request_id":"r1"}`,
		"NullClientID":        `{"value":"100","client_id":null,"request_id":"r1"}`,
		"NullRequestID":       `{"value":"100","client_id":"c1","request_id":null}`,
		"WrongCase":           `{"Value":"100","client_id":"c1","request_id":"r1"}`,
		"MissingClosingBrace": `{"value":"100","client_id":"c1","request_id":"r1"`,
		"TrailingComma":       `{"value":"100","client_id":"c1","request_id":"r1",}`,
		"TrailingGarbage":     `{"value":"100","client_id":"c1","request_id":"r1"} invalid`,
		"NestedValue":         `{"value":{"x":"100"},"client_id":"c1","request_id":"r1"}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			mock := &gatewayPutMock{}
			response := gatewayPutRequest(
				mock, "/kv/alpha", body,
			)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("status: %d", response.Code)
			}

			if mock.calls != 0 {
				t.Fatal("invalid JSON reached backend")
			}
		})
	}
}

func TestGatewayValidationDeleteNullFields(t *testing.T) {
	for _, body := range []string{
		`{"client_id":"c1","request_id":"r1","value":null}`,
		`{"client_id":"c1","request_id":"r1","expected":null}`,
	} {
		mock := &gatewayDeleteMock{}

		response := gatewayDeleteRequest(
			mock, "/kv/alpha", body,
		)

		if response.Code != http.StatusBadRequest ||
			mock.calls != 0 {
			t.Fatal("DELETE accepted an invalid null field")
		}
	}
}

func TestGatewayValidationReadRoutingErrors(t *testing.T) {
	cases := []struct {
		code shardraftpb.ErrorCode
		want int
		name string
	}{
		{
			shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
			400, "invalid_request",
		},
		{
			shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND,
			404, "key_not_found",
		},
		{
			shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			503, "leader_unavailable",
		},
		{
			shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			503, "leader_unavailable",
		},
		{
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
			503, "leader_unavailable",
		},
		{
			shardraftpb.ErrorCode_ERROR_CODE_INTERNAL,
			500, "internal_error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			response := httptest.NewRecorder()

			gatewayWriteReadError(
				response,
				&client.RoutingFailure{
					Detail: &shardraftpb.RoutingError{
						Code:    tc.code,
						Message: "private backend details",
					},
				},
			)

			if response.Code != tc.want {
				t.Fatalf("got %d, want %d",
					response.Code, tc.want)
			}

			var result gatewayErrorResponse

			if err := json.Unmarshal(
				response.Body.Bytes(), &result,
			); err != nil {
				t.Fatal(err)
			}

			if result.Code != tc.name {
				t.Fatalf("unexpected response: %+v", result)
			}

			if strings.Contains(
				response.Body.String(), "private backend",
			) {
				t.Fatal("backend details leaked")
			}
		})
	}
}

func TestGatewayValidationGRPCErrors(t *testing.T) {
	cases := []struct {
		code codes.Code
		want int
		name string
	}{
		{codes.InvalidArgument, 400, "invalid_request"},
		{codes.Canceled, 408, "request_canceled"},
		{codes.Unavailable, 503, "leader_unavailable"},
		{codes.DeadlineExceeded, 504, "deadline_exceeded"},
		{codes.Internal, 500, "internal_error"},
	}

	identity := client.MutationIdentity{
		ClientID:  "c1",
		RequestID: "r1",
	}

	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			err := status.Error(
				tc.code, "private backend details",
			)

			readResponse := httptest.NewRecorder()
			gatewayWriteReadError(readResponse, err)

			mutationResponse := httptest.NewRecorder()
			gatewayWriteMutationError(
				mutationResponse, err, identity,
			)

			if readResponse.Code != tc.want ||
				mutationResponse.Code != tc.want {
				t.Fatalf(
					"read=%d mutation=%d want=%d",
					readResponse.Code,
					mutationResponse.Code,
					tc.want,
				)
			}

			var readResult gatewayErrorResponse
			var mutationResult gatewayMutationErrorResponse

			if err := json.Unmarshal(
				readResponse.Body.Bytes(), &readResult,
			); err != nil {
				t.Fatal(err)
			}

			if err := json.Unmarshal(
				mutationResponse.Body.Bytes(), &mutationResult,
			); err != nil {
				t.Fatal(err)
			}

			if readResult.Code != tc.name ||
				mutationResult.Code != tc.name {
				t.Fatal("incorrect structured error code")
			}

			if mutationResult.ClientID != identity.ClientID ||
				mutationResult.RequestID != identity.RequestID {
				t.Fatal("mutation identity was lost")
			}

			for _, response := range []*httptest.ResponseRecorder{
				readResponse, mutationResponse,
			} {
				if strings.Contains(
					response.Body.String(), "private backend",
				) {
					t.Fatal("backend details leaked")
				}

				if response.Header().Get("Cache-Control") != "no-store" {
					t.Fatal("missing no-store header")
				}
			}
		})
	}
}

func TestGatewayValidationUnknownOutcomePriority(t *testing.T) {
	identity := client.MutationIdentity{
		ClientID:  "stable-client",
		RequestID: "stable-request",
	}

	for _, cause := range []error{
		context.DeadlineExceeded,
		context.Canceled,
		client.ErrRetryExhausted,
		status.Error(codes.Unavailable, "unavailable"),
	} {
		response := httptest.NewRecorder()

		gatewayWriteMutationError(
			response,
			errors.Join(client.ErrMutationOutcomeUnknown, cause),
			identity,
		)

		if response.Code != http.StatusGatewayTimeout {
			t.Fatalf("status: %d", response.Code)
		}

		var result gatewayMutationErrorResponse

		if err := json.Unmarshal(
			response.Body.Bytes(), &result,
		); err != nil {
			t.Fatal(err)
		}

		if result.Code != "outcome_unknown" ||
			result.ClientID != identity.ClientID ||
			result.RequestID != identity.RequestID {
			t.Fatalf("unexpected response: %+v", result)
		}
	}
}
