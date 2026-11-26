package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMultiTransportHTTPValidation(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		sender *multiRaftTransport
		want   int
	}{
		{
			"invalid shard",
			"GET",
			"/api/v1/shards/abc/transport",
			nil,
			400,
		},
		{
			"zero shard",
			"GET",
			"/api/v1/shards/0/transport",
			nil,
			400,
		},
		{
			"invalid metadata group",
			"GET",
			"/api/v1/shards/5/transport",
			nil,
			400,
		},
		{
			"unavailable transport",
			"GET",
			"/api/v1/shards/1/transport",
			nil,
			503,
		},
		{
			"closed transport",
			"GET",
			"/api/v1/shards/1/transport",
			&multiRaftTransport{closed: true},
			503,
		},
		{
			"non-local shard",
			"GET",
			"/api/v1/shards/1/transport",
			&multiRaftTransport{},
			404,
		},
		{
			"reject mutation",
			"POST",
			"/api/v1/shards/1/transport",
			nil,
			405,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()

			registerMultiTransportHTTP(
				mux,
				tc.sender,
				1,
			)

			request := httptest.NewRequest(
				tc.method,
				tc.path,
				nil,
			)

			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != tc.want {
				t.Fatalf(
					"expected HTTP %d, got %d: %s",
					tc.want,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}
