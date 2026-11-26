package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMultiMetadataHTTPNonReplica(t *testing.T) {
	mux := http.NewServeMux()

	registerMultiMetadataHTTP(
		mux,
		nil,
		nil,
		4,
	)

	paths := []string{
		"/api/v1/metadata/status",
		"/api/v1/metadata/shard-map",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodGet,
				path,
				nil,
			)

			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusNotFound {
				t.Fatalf(
					"expected HTTP 404, got %d",
					response.Code,
				)
			}

			if !strings.Contains(
				response.Body.String(),
				"node does not host metadata group",
			) {
				t.Fatalf(
					"unexpected response: %s",
					response.Body.String(),
				)
			}

			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing no-store header")
			}
		})
	}
}

func TestMultiMetadataHTTPReadOnly(t *testing.T) {
	mux := http.NewServeMux()

	registerMultiMetadataHTTP(
		mux,
		nil,
		nil,
		4,
	)

	paths := []string{
		"/api/v1/metadata/status",
		"/api/v1/metadata/shard-map",
	}

	methods := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
	}

	for _, path := range paths {
		for _, method := range methods {
			t.Run(method+path, func(t *testing.T) {
				request := httptest.NewRequest(
					method,
					path,
					nil,
				)

				response := httptest.NewRecorder()

				mux.ServeHTTP(response, request)

				if response.Code != http.StatusMethodNotAllowed {
					t.Fatalf(
						"expected HTTP 405, got %d",
						response.Code,
					)
				}
			})
		}
	}
}

func TestMultiMetadataHTTPUnknownRoute(t *testing.T) {
	mux := http.NewServeMux()

	registerMultiMetadataHTTP(
		mux,
		nil,
		nil,
		4,
	)

	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metadata/unknown",
		nil,
	)

	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf(
			"expected HTTP 404, got %d",
			response.Code,
		)
	}
}
