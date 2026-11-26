package main

import (
	"net/http/httptest"
	"testing"
)

func TestControlValidation(t *testing.T) {
	handler := newHandler()

	tests := []struct {
		name   string
		method string
		path   string
		origin string
		want   int
	}{
		{
			"health",
			"GET",
			"/health",
			"",
			200,
		},
		{
			"invalid node",
			"POST",
			"/api/v1/control/nodes/6/stop",
			"",
			400,
		},
		{
			"invalid node format",
			"POST",
			"/api/v1/control/nodes/abc/stop",
			"",
			400,
		},
		{
			"invalid action",
			"POST",
			"/api/v1/control/nodes/1/remove",
			"",
			400,
		},
		{
			"reject GET mutation",
			"GET",
			"/api/v1/control/nodes/1/stop",
			"",
			405,
		},
		{
			"reject external origin",
			"POST",
			"/api/v1/control/nodes/1/stop",
			"https://example.com",
			403,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(
				tc.method,
				tc.path,
				nil,
			)

			if tc.origin != "" {
				request.Header.Set("Origin", tc.origin)
			}

			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != tc.want {
				t.Fatalf(
					"expected %d, got %d: %s",
					tc.want,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}
