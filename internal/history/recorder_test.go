package history

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestRecorderSequentialOperations(t *testing.T) {
	r := NewRecorder()

	putID, err := r.Begin(Input{
		ClientID:  "client-1",
		RequestID: "request-1",
		Kind:      Put,
		Key:       "key-1",
		Value:     "value-1",
	})

	if err != nil {
		t.Fatal(err)
	}

	if err := r.Complete(putID, Output{
		Applied: true,
		Outcome: Success,
	}); err != nil {
		t.Fatal(err)
	}

	getID, err := r.Begin(Input{
		ClientID: "client-1",
		Kind:     Get,
		Key:      "key-1",
	})

	if err != nil {
		t.Fatal(err)
	}

	if err := r.Complete(getID, Output{
		Value:   "value-1",
		Found:   true,
		Outcome: Success,
	}); err != nil {
		t.Fatal(err)
	}

	h := r.Snapshot()

	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}

	if len(h.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(h.Operations))
	}

	put := h.Operations[0]
	get := h.Operations[1]

	if put.ReturnNS >= get.CallNS {
		t.Fatal("sequential event ordering was not preserved")
	}

	if get.Output.Value != "value-1" || !get.Output.Found {
		t.Fatal("read result was not preserved")
	}
}

func TestRecorderOverlappingOperations(t *testing.T) {
	r := NewRecorder()

	first, err := r.Begin(Input{
		ClientID: "client-1",
		Kind:     Get,
		Key:      "key",
	})

	if err != nil {
		t.Fatal(err)
	}

	second, err := r.Begin(Input{
		ClientID: "client-2",
		Kind:     Get,
		Key:      "key",
	})

	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []uint64{second, first} {
		if err := r.Complete(id, Output{
			Outcome: Success,
		}); err != nil {
			t.Fatal(err)
		}
	}

	h := r.Snapshot()

	a := h.Operations[0]
	b := h.Operations[1]

	if !(a.CallNS < b.CallNS &&
		b.CallNS < b.ReturnNS &&
		b.ReturnNS < a.ReturnNS) {
		t.Fatal("overlapping event ordering was not preserved")
	}
}

func TestRecorderConcurrentOperations(t *testing.T) {
	r := NewRecorder()

	const workers = 8
	const operations = 100

	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()

			for i := 0; i < operations; i++ {
				id, err := r.Begin(Input{
					ClientID: fmt.Sprintf("client-%d", worker),
					Kind:     Get,
					Key:      fmt.Sprintf("key-%d", i%4),
				})

				if err != nil {
					t.Error(err)
					return
				}

				if err := r.Complete(id, Output{
					Outcome: Success,
				}); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}

	wg.Wait()

	if r.Len() != workers*operations {
		t.Fatalf("unexpected operation count: %d", r.Len())
	}

	h := r.Snapshot()

	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}

	timestamps := make(map[int64]bool)

	for _, op := range h.Operations {
		if op.Output.Outcome != Success {
			t.Fatal("operation not completed")
		}

		for _, ts := range []int64{op.CallNS, op.ReturnNS} {
			if timestamps[ts] {
				t.Fatalf("duplicate timestamp: %d", ts)
			}

			timestamps[ts] = true
		}
	}

	t.Logf("recorded %d concurrent operations", r.Len())
}

func TestRecorderOutcomePreservation(t *testing.T) {
	r := NewRecorder()

	for i, outcome := range []Outcome{
		Success,
		Failed,
		Unknown,
		Pending,
	} {
		id, err := r.Begin(Input{
			ClientID:  "client",
			RequestID: fmt.Sprintf("request-%d", i),
			Kind:      Put,
			Key:       "key",
			Value:     "value",
		})

		if err != nil {
			t.Fatal(err)
		}

		if outcome == Pending {
			continue
		}

		output := Output{
			Outcome: outcome,
			Applied: outcome == Success,
		}

		if outcome != Success {
			output.Error = "simulated request error"
		}

		if err := r.Complete(id, output); err != nil {
			t.Fatal(err)
		}
	}

	h := r.Snapshot()

	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}

	expected := []Outcome{
		Success,
		Failed,
		Unknown,
		Pending,
	}

	for i, outcome := range expected {
		if h.Operations[i].Output.Outcome != outcome {
			t.Fatalf("outcome %d was not preserved", i)
		}
	}
}

func TestRecorderJSONRoundTrip(t *testing.T) {
	r := NewRecorder()

	id, err := r.Begin(Input{
		ClientID:  "client",
		RequestID: "request",
		Kind:      CAS,
		Key:       "key",
		Expected:  "old",
		Value:     "new",
	})

	if err != nil {
		t.Fatal(err)
	}

	if err := r.Complete(id, Output{
		Applied: true,
		Value:   "new",
		Outcome: Success,
	}); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(
		t.TempDir(),
		"results",
		"history.json",
	)

	if err := r.Save(path); err != nil {
		t.Fatal(err)
	}

	loaded, err := Load(path)

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(r.Snapshot(), loaded) {
		t.Fatal("history changed after JSON round trip")
	}
}

func TestRecorderRejectsInvalidOperations(t *testing.T) {
	r := NewRecorder()

	invalid := []Input{
		{},
		{ClientID: "client", Kind: Get},
		{ClientID: "client", Kind: Put, Key: "key"},
		{ClientID: "client", Kind: "invalid", Key: "key"},
	}

	for _, input := range invalid {
		if _, err := r.Begin(input); err == nil {
			t.Fatalf("accepted invalid input: %+v", input)
		}
	}

	id, err := r.Begin(Input{
		ClientID: "client",
		Kind:     Get,
		Key:      "key",
	})

	if err != nil {
		t.Fatal(err)
	}

	if err := r.Complete(id, Output{
		Outcome: Pending,
	}); err == nil {
		t.Fatal("accepted pending completion")
	}

	if err := r.Complete(999, Output{
		Outcome: Success,
	}); err == nil {
		t.Fatal("accepted unknown operation")
	}

	output := Output{Outcome: Success}

	if err := r.Complete(id, output); err != nil {
		t.Fatal(err)
	}

	if err := r.Complete(id, output); err == nil {
		t.Fatal("accepted duplicate completion")
	}
}

func TestRecorderSnapshotIsolation(t *testing.T) {
	r := NewRecorder()

	_, err := r.Begin(Input{
		ClientID: "client",
		Kind:     Get,
		Key:      "original",
	})

	if err != nil {
		t.Fatal(err)
	}

	h := r.Snapshot()
	h.Operations[0].Input.Key = "modified"

	if r.Snapshot().Operations[0].Input.Key != "original" {
		t.Fatal("snapshot modified recorder state")
	}
}

func TestDecodeRejectsInvalidHistory(t *testing.T) {
	cases := []string{
		`{}`,
		`{"version":999,"operations":[]}`,
		`{"version":1,"operations":[]} {}`,
		`{"version":1,"operations":[],"unexpected":true}`,
		`not-json`,
	}

	for _, input := range cases {
		if _, err := Decode(bytes.NewBufferString(input)); err == nil {
			t.Fatalf("accepted invalid history: %s", input)
		}
	}
}
