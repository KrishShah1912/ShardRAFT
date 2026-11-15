package kvstore

import "testing"

func TestPutOperation(t *testing.T) {
	store := New()

	err := store.Apply(makeEntry(t, 1, Command{
		Op: OpPut, Key: "name", Value: "ShardRAFT",
	}))
	if err != nil {
		t.Fatal(err)
	}

	value, ok := store.Get("name")
	if !ok || value != "ShardRAFT" {
		t.Fatalf("unexpected value: %q, exists=%v", value, ok)
	}
}

func TestCompareAndSwapSuccess(t *testing.T) {
	store := New()

	commands := []Command{
		{Op: OpPut, Key: "counter", Value: "10"},
		{Op: OpCompareAndSwap, Key: "counter", Expected: "10", Value: "20"},
	}

	for i, command := range commands {
		if err := store.Apply(makeEntry(t, uint64(i+1), command)); err != nil {
			t.Fatal(err)
		}
	}

	value, _ := store.Get("counter")
	if value != "20" {
		t.Fatalf("expected 20, got %q", value)
	}
}

func TestCompareAndSwapMismatch(t *testing.T) {
	store := New()

	commands := []Command{
		{Op: OpPut, Key: "counter", Value: "10"},
		{Op: OpCompareAndSwap, Key: "counter", Expected: "99", Value: "20"},
	}

	for i, command := range commands {
		if err := store.Apply(makeEntry(t, uint64(i+1), command)); err != nil {
			t.Fatal(err)
		}
	}

	value, _ := store.Get("counter")
	if value != "10" {
		t.Fatalf("CAS mismatch changed value: %q", value)
	}
}

func TestCompareAndSwapMissingKey(t *testing.T) {
	store := New()

	err := store.Apply(makeEntry(t, 1, Command{
		Op:       OpCompareAndSwap,
		Key:      "missing",
		Expected: "",
		Value:    "created",
	}))
	if err != nil {
		t.Fatal(err)
	}

	if _, exists := store.Get("missing"); exists {
		t.Fatal("CAS unexpectedly created a missing key")
	}
}
