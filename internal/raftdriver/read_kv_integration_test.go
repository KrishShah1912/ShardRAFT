package raftdriver

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
)

type readKVResult struct {
	state   raft.ReadState
	value   string
	found   bool
	applied uint64
}

type readKVFixture struct {
	node    *raft.Raft
	driver  *Driver
	store   *kvstore.Store
	results []readKVResult
}

func newReadKVFixture(t *testing.T) *readKVFixture {
	t.Helper()

	return newReadKVFixtureWithStore(t, kvstore.New())
}

func newReadKVFixtureWithStore(
	t *testing.T,
	store *kvstore.Store,
) *readKVFixture {
	t.Helper()

	path := filepath.Join(t.TempDir(), "read-kv.wal")

	node, storage, driver, err := RecoverDriver(
		path,
		1,
		nil,
		5,
		store.Apply,
		func(messages []raft.Message) error {
			if len(messages) != 0 {
				return fmt.Errorf("unexpected single-node messages")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Error(err)
		}
	})

	f := &readKVFixture{
		node:   node,
		driver: driver,
		store:  store,
	}

	if err := driver.SetReadStateAppliedIndex(
		store.AppliedIndex,
	); err != nil {
		t.Fatal(err)
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			applied := store.AppliedIndex()

			if applied < state.Index {
				return fmt.Errorf(
					"read barrier %d exceeds applied index %d",
					state.Index, applied,
				)
			}

			value, found := store.Get(string(state.Context))

			f.results = append(f.results, readKVResult{
				state:   state,
				value:   value,
				found:   found,
				applied: applied,
			})

			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		node.Tick()
		f.process(t)

		if node.Status().Role == raft.Leader &&
			store.AppliedIndex() > 0 {
			return f
		}
	}

	t.Fatal("single-node leader election failed")
	return nil
}

func (f *readKVFixture) process(t *testing.T) {
	t.Helper()

	if err := f.driver.Process(); err != nil {
		t.Fatal(err)
	}
}

func (f *readKVFixture) propose(
	t *testing.T,
	command kvstore.Command,
) {
	t.Helper()

	data, err := kvstore.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}

	if err := f.node.Propose(data); err != nil {
		t.Fatal(err)
	}
}

func (f *readKVFixture) put(
	t *testing.T,
	key, value string,
) {
	t.Helper()

	f.propose(t, kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   key,
		Value: value,
	})

	f.process(t)
}

func (f *readKVFixture) submit(
	t *testing.T,
	key string,
) {
	t.Helper()

	if err := f.node.ReadIndex([]byte(key)); err != nil {
		t.Fatal(err)
	}
}

func (f *readKVFixture) requireRead(
	t *testing.T,
	key, expected string,
	found bool,
) readKVResult {
	t.Helper()

	before := len(f.results)

	f.submit(t, key)
	f.process(t)

	if len(f.results) != before+1 {
		t.Fatalf(
			"expected one new confirmation, got %d",
			len(f.results)-before,
		)
	}

	result := f.results[before]

	if result.value != expected || result.found != found {
		t.Fatalf(
			"key %q: got (%q, %v), want (%q, %v)",
			key, result.value, result.found,
			expected, found,
		)
	}

	if result.state.RequestID == 0 ||
		result.state.Term != f.node.Status().Term ||
		result.state.Index == 0 ||
		result.applied < result.state.Index {
		t.Fatalf("invalid read confirmation: %+v", result)
	}

	if string(result.state.Context) != key {
		t.Fatal("incorrect read context")
	}

	return result
}

func TestDriverReadKVInitialMissingKey(t *testing.T) {
	f := newReadKVFixture(t)

	f.requireRead(t, "missing", "", false)
}

func TestDriverReadKVPut(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "alpha", "100")

	result := f.requireRead(t, "alpha", "100", true)

	if result.state.Index != f.store.AppliedIndex() {
		t.Fatal("read did not capture current applied commit")
	}
}

func TestDriverReadKVOverwrite(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "alpha", "100")
	first := f.requireRead(t, "alpha", "100", true)

	f.put(t, "alpha", "200")
	second := f.requireRead(t, "alpha", "200", true)

	if second.state.Index <= first.state.Index {
		t.Fatal("updated read barrier did not advance")
	}

	if second.state.RequestID == first.state.RequestID {
		t.Fatal("request identity was reused")
	}
}

func TestDriverReadKVDelete(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "alpha", "100")
	f.requireRead(t, "alpha", "100", true)

	f.propose(t, kvstore.Command{
		Op:  kvstore.OpDelete,
		Key: "alpha",
	})

	f.process(t)
	f.requireRead(t, "alpha", "", false)
}

func TestDriverReadKVCASSuccess(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "counter", "10")

	f.propose(t, kvstore.Command{
		Op:        kvstore.OpCompareAndSwap,
		Key:       "counter",
		Expected:  "10",
		Value:     "20",
		ClientID:  "client",
		RequestID: "cas-success",
	})

	f.process(t)
	f.requireRead(t, "counter", "20", true)

	result, found := f.store.RequestResult(
		"client", "cas-success",
	)

	if !found || !result.Applied || result.Value != "20" {
		t.Fatalf("incorrect CAS result: %+v", result)
	}
}

func TestDriverReadKVCASMismatch(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "counter", "10")

	f.propose(t, kvstore.Command{
		Op:        kvstore.OpCompareAndSwap,
		Key:       "counter",
		Expected:  "99",
		Value:     "20",
		ClientID:  "client",
		RequestID: "cas-mismatch",
	})

	f.process(t)
	f.requireRead(t, "counter", "10", true)

	result, found := f.store.RequestResult(
		"client", "cas-mismatch",
	)

	if !found || result.Applied {
		t.Fatalf("incorrect failed CAS result: %+v", result)
	}
}

func TestDriverReadKVSameReadyApplication(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "alpha", "old")

	before := f.store.AppliedIndex()

	f.propose(t, kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   "alpha",
		Value: "new",
	})

	f.submit(t, "alpha")

	if f.store.AppliedIndex() != before {
		t.Fatal("application advanced before processing")
	}

	value, _ := f.store.Get("alpha")
	if value != "old" {
		t.Fatal("unprocessed write changed KV state")
	}

	ready := f.node.Ready()

	if len(ready.CommittedEntries) == 0 ||
		len(ready.ReadStates) != 1 {
		t.Fatal("expected write and confirmation in same Ready")
	}

	if len(f.results) != 0 {
		t.Fatal("read delivered before driver processing")
	}

	f.process(t)

	if len(f.results) != 1 ||
		f.results[0].value != "new" ||
		!f.results[0].found {
		t.Fatal("read did not observe newly applied write")
	}

	if f.results[0].applied <= before {
		t.Fatal("applied index did not advance")
	}
}

func TestDriverReadKVApplicationLagRecovery(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "alpha", "old")

	// Model an external application queue while retaining
	// the actual KV store as the applied-index provider.
	var pending []raft.Entry

	f.driver.apply = func(entry raft.Entry) error {
		pending = append(pending, entry)
		return nil
	}

	before := f.store.AppliedIndex()

	f.propose(t, kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   "alpha",
		Value: "new",
	})

	f.submit(t, "alpha")
	f.process(t)

	if len(pending) != 1 {
		t.Fatal("expected one deferred application")
	}

	if f.store.AppliedIndex() != before {
		t.Fatal("actual KV application advanced prematurely")
	}

	if len(f.results) != 0 {
		t.Fatal("application lag released confirmation")
	}

	if len(f.node.Ready().ReadStates) != 1 {
		t.Fatal("deferred confirmation was lost")
	}

	for i := 0; i < 10; i++ {
		f.process(t)
	}

	if len(f.results) != 0 {
		t.Fatal("repeated processing bypassed application barrier")
	}

	for _, entry := range pending {
		if err := f.store.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}

	f.driver.apply = f.store.Apply

	f.process(t)

	if len(f.results) != 1 ||
		f.results[0].value != "new" ||
		!f.results[0].found {
		t.Fatal("application recovery did not release correct read")
	}

	if len(f.node.Ready().ReadStates) != 0 {
		t.Fatal("recovered confirmation was not acknowledged")
	}
}

func TestDriverReadKVConcurrentConfirmations(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "alpha", "100")
	f.put(t, "beta", "200")

	keys := []string{
		"alpha", "beta", "missing", "alpha", "beta",
	}

	for _, key := range keys {
		f.submit(t, key)
	}

	f.process(t)

	if len(f.results) != len(keys) {
		t.Fatalf("expected %d confirmations", len(keys))
	}

	seen := make(map[uint64]bool)

	for i, result := range f.results {
		if seen[result.state.RequestID] {
			t.Fatal("duplicate request identity")
		}

		seen[result.state.RequestID] = true

		if string(result.state.Context) != keys[i] {
			t.Fatal("confirmation context mismatch")
		}

		expected, found := f.store.Get(keys[i])

		if result.value != expected || result.found != found {
			t.Fatal("incorrect KV result")
		}
	}
}

func TestDriverReadKVNoDuplicateDelivery(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "alpha", "100")

	f.requireRead(t, "alpha", "100", true)

	for i := 0; i < 100; i++ {
		f.process(t)
	}

	if len(f.results) != 1 {
		t.Fatal("confirmation delivered more than once")
	}

	if len(f.node.Ready().ReadStates) != 0 {
		t.Fatal("acknowledged confirmation remained queued")
	}
}

func TestDriverReadKVApplicationFailure(t *testing.T) {
	f := newReadKVFixture(t)

	f.put(t, "alpha", "100")

	before := f.store.AppliedIndex()

	// Invalid encoded command causes the real KV Apply
	// implementation to fail.
	if err := f.node.Propose(
		[]byte("invalid-json-command"),
	); err != nil {
		t.Fatal(err)
	}

	f.submit(t, "alpha")

	if err := f.driver.Process(); err == nil {
		t.Fatal("expected actual KV application failure")
	}

	if !f.driver.Stopped() {
		t.Fatal("driver did not stop after application failure")
	}

	if len(f.results) != 0 {
		t.Fatal("failed application released confirmation")
	}

	if f.store.AppliedIndex() != before {
		t.Fatal("failed application advanced KV index")
	}

	value, found := f.store.Get("alpha")
	if !found || value != "100" {
		t.Fatal("failed application modified existing KV state")
	}

	if len(f.node.Ready().ReadStates) != 1 {
		t.Fatal("failed application acknowledged confirmation")
	}
}

func TestDriverReadKVNoDurableChanges(t *testing.T) {
	f := newReadKVFixture(t)

	f.propose(t, kvstore.Command{
		Op:        kvstore.OpPut,
		Key:       "alpha",
		Value:     "100",
		ClientID:  "client",
		RequestID: "write-1",
	})

	f.process(t)

	status := f.node.Status()
	index, data, dedup := f.store.CaptureSnapshot()
	logBefore := f.node.DebugLog()

	for i := 0; i < 100; i++ {
		f.submit(t, "alpha")

		ready := f.node.Ready()

		if ready.HardState != nil ||
			len(ready.Entries) != 0 ||
			len(ready.CommittedEntries) != 0 ||
			ready.TruncateFrom != 0 {
			t.Fatal("read generated durable Raft modifications")
		}

		f.process(t)
	}

	if len(f.results) != 100 {
		t.Fatal("incorrect confirmation count")
	}

	afterIndex, afterData, afterDedup :=
		f.store.CaptureSnapshot()

	if index != afterIndex ||
		!reflect.DeepEqual(data, afterData) ||
		!reflect.DeepEqual(dedup, afterDedup) {
		t.Fatal("reads modified KV state or deduplication history")
	}

	if !reflect.DeepEqual(status, f.node.Status()) ||
		!reflect.DeepEqual(logBefore, f.node.DebugLog()) {
		t.Fatal("reads modified Raft status or log")
	}
}

func TestDriverReadKVPebbleIntegration(t *testing.T) {
	engine, err := kvstore.OpenPebble(
		filepath.Join(t.TempDir(), "pebble"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	})

	store, err := kvstore.NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}

	f := newReadKVFixtureWithStore(t, store)

	f.put(t, "alpha", "100")
	f.requireRead(t, "alpha", "100", true)

	value, found, err := engine.Get("alpha")
	if err != nil || !found || value != "100" {
		t.Fatalf("incorrect Pebble value: %q, %v, %v",
			value, found, err)
	}

	f.put(t, "alpha", "200")
	f.requireRead(t, "alpha", "200", true)

	value, found, err = engine.Get("alpha")
	if err != nil || !found || value != "200" {
		t.Fatal("Pebble overwrite not visible")
	}

	f.propose(t, kvstore.Command{
		Op:  kvstore.OpDelete,
		Key: "alpha",
	})

	f.process(t)
	f.requireRead(t, "alpha", "", false)

	_, found, err = engine.Get("alpha")
	if err != nil || found {
		t.Fatal("Pebble deletion not visible")
	}
}
