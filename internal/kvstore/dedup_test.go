package kvstore

import "testing"

func TestDuplicateCASReturnsOriginalSuccess(t *testing.T) {
	store := New()

	commands := []Command{
		{
			Op: OpPut, Key: "counter", Value: "10",
		},
		{
			Op:  OpCompareAndSwap,
			Key: "counter", Expected: "10", Value: "20",
			ClientID: "client-1", RequestID: "request-1",
		},
		{
			Op:  OpCompareAndSwap,
			Key: "counter", Expected: "10", Value: "20",
			ClientID: "client-1", RequestID: "request-1",
		},
	}

	for i, command := range commands {
		if err := store.Apply(
			makeEntry(t, uint64(i+1), command),
		); err != nil {
			t.Fatal(err)
		}
	}

	value, _ := store.Get("counter")
	if value != "20" {
		t.Fatalf("expected 20, got %q", value)
	}

	result, found := store.RequestResult(
		"client-1", "request-1",
	)
	if !found || !result.Applied || result.Value != "20" {
		t.Fatalf("unexpected original result: %+v, found=%v",
			result, found)
	}

	if store.AppliedIndex() != 3 {
		t.Fatal("duplicate log entry did not advance applied index")
	}
}

func TestRejectReusedRequestIDWithDifferentCommand(t *testing.T) {
	store := New()

	first := Command{
		Op: OpPut, Key: "x", Value: "10",
		ClientID: "client-1", RequestID: "request-1",
	}
	second := Command{
		Op: OpPut, Key: "x", Value: "99",
		ClientID: "client-1", RequestID: "request-1",
	}

	if err := store.Apply(makeEntry(t, 1, first)); err != nil {
		t.Fatal(err)
	}

	if err := store.Apply(makeEntry(t, 2, second)); err != nil {
		t.Fatalf("committed conflicting entry blocked application: %v", err)
	}

	if store.AppliedIndex() != 2 {
		t.Fatal("conflicting entry did not advance applied index")
	}

	value, _ := store.Get("x")
	if value != "10" {
		t.Fatalf("conflicting request changed value: %q", value)
	}
}

func TestRequestIdentifiersMustBePaired(t *testing.T) {
	_, err := EncodeCommand(Command{
		Op: OpPut, Key: "x", Value: "10",
		ClientID: "client-1",
	})
	if err == nil {
		t.Fatal("expected missing request ID error")
	}
}
