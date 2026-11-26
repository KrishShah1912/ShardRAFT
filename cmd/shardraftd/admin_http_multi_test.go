package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMultiRaftAdminHTTPValidation(t *testing.T) {
	mux := http.NewServeMux()

	registerMultiRaftAdminHTTP(mux, nil, 1)

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{
			"invalid shard",
			"GET",
			"/api/v1/shards/abc/raft",
			400,
		},
		{
			"zero shard",
			"GET",
			"/api/v1/shards/0/raft",
			400,
		},
		{
			"metadata is not a data shard",
			"GET",
			"/api/v1/shards/5/raft",
			400,
		},
		{
			"raft unavailable",
			"GET",
			"/api/v1/shards/1/raft",
			503,
		},
		{
			"log unavailable",
			"GET",
			"/api/v1/shards/1/raft/log",
			503,
		},
		{
			"kv unavailable",
			"GET",
			"/api/v1/shards/1/kv",
			503,
		},
		{
			"storage unavailable",
			"GET",
			"/api/v1/shards/1/storage",
			503,
		},
		{
			"reject mutation",
			"POST",
			"/api/v1/shards/1/raft",
			405,
		},
		{
			"reject delete",
			"DELETE",
			"/api/v1/shards/1/kv",
			405,
		},
		{
			"unknown route",
			"GET",
			"/api/v1/shards/1/unknown",
			404,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
