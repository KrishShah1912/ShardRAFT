package kvstore

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func TestAtomicSnapshotConsistency(t *testing.T) {
	store := New()

	const total = 100

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 1; i <= total; i++ {
			command := Command{
				Op:        OpPut,
				Key:       "counter",
				Value:     strconv.Itoa(i),
				ClientID:  "client-1",
				RequestID: fmt.Sprintf("request-%d", i),
			}

			data, err := EncodeCommand(command)
			if err != nil {
				t.Errorf("encode: %v", err)
				return
			}

			if err := store.Apply(raft.Entry{
				Index: uint64(i),
				Term:  1,
				Type:  raft.EntryNormal,
				Data:  data,
			}); err != nil {
				t.Errorf("apply: %v", err)
				return
			}
		}
	}()

	check := func() {
		t.Helper()

		index, data, dedup := store.CaptureSnapshot()

		if index == 0 {
			if len(data) != 0 || len(dedup) != 0 {
				t.Fatal("initial snapshot is inconsistent")
			}
			return
		}

		expected := strconv.FormatUint(index, 10)

		if data["counter"] != expected {
			t.Fatalf(
				"snapshot index %d has counter=%q",
				index, data["counter"],
			)
		}

		if len(dedup) != int(index) {
			t.Fatalf(
				"snapshot index %d has %d dedup records",
				index, len(dedup),
			)
		}
	}

	for i := 0; i < 1000; i++ {
		check()
	}

	wg.Wait()
	check()

	if store.AppliedIndex() != total {
		t.Fatalf(
			"expected final index %d, got %d",
			total, store.AppliedIndex(),
		)
	}
}
