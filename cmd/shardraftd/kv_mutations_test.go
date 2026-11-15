package main

import (
	"net/http"
	"testing"
)

func TestHTTPKVCAS(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	put := `{"value":"Go","client_id":"c1","request_id":"put-1"}`
	rec := sendKVRequest(t, handler, http.MethodPut, "/kv/language", put)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT failed: %d %s", rec.Code, rec.Body.String())
	}

	cas := `{"expected":"Go","value":"Python","client_id":"c1","request_id":"cas-1"}`
	rec = sendKVRequest(t, handler, http.MethodPost, "/kv/language/cas", cas)
	if rec.Code != http.StatusOK {
		t.Fatalf("CAS failed: %d %s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "{\"applied\":true,\"value\":\"Python\"}\n" {
		t.Fatalf("unexpected CAS result: %s", got)
	}

	rec = sendKVRequest(t, handler, http.MethodGet, "/kv/language", "")
	if rec.Code != http.StatusOK || rec.Body.String() != "{\"key\":\"language\",\"value\":\"Python\"}\n" {
		t.Fatalf("unexpected GET result: %d %s", rec.Code, rec.Body.String())
	}
}

func TestHTTPKVCASFailure(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	put := `{"value":"Go","client_id":"c1","request_id":"put-1"}`
	rec := sendKVRequest(t, handler, http.MethodPut, "/kv/language", put)
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}

	cas := `{"expected":"Java","value":"Python","client_id":"c1","request_id":"cas-fail"}`
	rec = sendKVRequest(t, handler, http.MethodPost, "/kv/language/cas", cas)
	if rec.Code != http.StatusOK {
		t.Fatalf("CAS status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "{\"applied\":false}\n" {
		t.Fatalf("unexpected failed CAS result: %s", got)
	}

	rec = sendKVRequest(t, handler, http.MethodGet, "/kv/language", "")
	if rec.Code != http.StatusOK || rec.Body.String() != "{\"key\":\"language\",\"value\":\"Go\"}\n" {
		t.Fatalf("failed CAS changed the value: %s", rec.Body.String())
	}
}

func TestHTTPKVDelete(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	put := `{"value":"Go","client_id":"c1","request_id":"put-1"}`
	rec := sendKVRequest(t, handler, http.MethodPut, "/kv/language", put)
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}

	del := `{"client_id":"c1","request_id":"delete-1"}`
	rec = sendKVRequest(t, handler, http.MethodDelete, "/kv/language", del)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "{\"applied\":true}\n" {
		t.Fatalf("unexpected DELETE result: %s", got)
	}

	rec = sendKVRequest(t, handler, http.MethodGet, "/kv/language", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("deleted key still exists: %d", rec.Code)
	}
}

func TestHTTPKVMutationDedup(t *testing.T) {
	node := newLeaderForHTTPTest(t)
	handler := statusHandler(node)

	put := `{"value":"Go","client_id":"c1","request_id":"put-1"}`
	rec := sendKVRequest(t, handler, http.MethodPut, "/kv/language", put)
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}

	cas := `{"expected":"Go","value":"Python","client_id":"c1","request_id":"cas-1"}`

	for i := 0; i < 2; i++ {
		rec = sendKVRequest(t, handler, http.MethodPost, "/kv/language/cas", cas)
		if rec.Code != http.StatusOK {
			t.Fatalf("CAS retry %d failed: %s", i, rec.Body.String())
		}
		if rec.Body.String() != "{\"applied\":true,\"value\":\"Python\"}\n" {
			t.Fatalf("unexpected CAS retry result: %s", rec.Body.String())
		}
	}

	conflict := `{"expected":"Go","value":"Java","client_id":"c1","request_id":"cas-1"}`
	rec = sendKVRequest(t, handler, http.MethodPost, "/kv/language/cas", conflict)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}
