package raftdriver

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	"github.com/KrishShah1912/shardraft/internal/snapshot"
)

type readRecoveryFixture struct {
	t        *testing.T
	walPath  string
	snapPath string
	node     *raft.Raft
	driver   *Driver
	store    *kvstore.Store
	closeWAL func() error
	compact  func(uint64, uint64) error
	results  []readKVResult
}

func newReadRecoveryFixture(t *testing.T) *readRecoveryFixture {
	t.Helper()

	dir := t.TempDir()

	f := &readRecoveryFixture{
		t:        t,
		walPath:  filepath.Join(dir, "node.wal"),
		snapPath: filepath.Join(dir, "state.snapshot"),
	}

	t.Cleanup(func() {
		if f.closeWAL != nil {
			if err := f.closeWAL(); err != nil {
				t.Error(err)
			}
		}
	})

	f.open()
	f.elect()

	return f
}

func (f *readRecoveryFixture) open() {
	f.t.Helper()

	node, storage, driver, store, err := RecoverKVDriver(
		f.walPath,
		f.snapPath,
		1,
		nil,
		5,
		func(messages []raft.Message) error {
			if len(messages) != 0 {
				return fmt.Errorf("unexpected single-node messages")
			}
			return nil
		},
	)
	if err != nil {
		f.t.Fatal(err)
	}

	f.node = node
	f.driver = driver
	f.store = store
	f.closeWAL = storage.Close
	f.compact = storage.CompactPrefix
	f.results = nil

	if err := driver.SetReadStateAppliedIndex(
		store.AppliedIndex,
	); err != nil {
		f.t.Fatal(err)
	}

	if err := driver.SetReadStateHandler(
		func(state raft.ReadState) error {
			applied := store.AppliedIndex()

			if applied < state.Index {
				return fmt.Errorf("application barrier not satisfied")
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
		f.t.Fatal(err)
	}
}

func (f *readRecoveryFixture) process() {
	f.t.Helper()

	if err := f.driver.Process(); err != nil {
		f.t.Fatal(err)
	}
}

func (f *readRecoveryFixture) elect() {
	f.t.Helper()

	for i := 0; i < 50; i++ {
		f.node.Tick()
		f.process()

		status := f.node.Status()

		if status.Role == raft.Leader &&
			status.Commit > 0 &&
			f.store.AppliedIndex() >= status.Commit {

			term, found := f.node.TermAt(status.Commit)

			if found && term == status.Term {
				return
			}
		}
	}

	f.t.Fatal("leader election failed")
}

func (f *readRecoveryFixture) restart() {
	f.t.Helper()

	if err := f.closeWAL(); err != nil {
		f.t.Fatal(err)
	}

	f.closeWAL = nil

	f.open()
}

func (f *readRecoveryFixture) propose(command kvstore.Command) {
	f.t.Helper()

	data, err := kvstore.EncodeCommand(command)
	if err != nil {
		f.t.Fatal(err)
	}

	if err := f.node.Propose(data); err != nil {
		f.t.Fatal(err)
	}

	f.process()
}

func (f *readRecoveryFixture) put(key, value string) {
	f.t.Helper()

	f.propose(kvstore.Command{
		Op:    kvstore.OpPut,
		Key:   key,
		Value: value,
	})
}

func (f *readRecoveryFixture) submit(key string) {
	f.t.Helper()

	if err := f.node.ReadIndex([]byte(key)); err != nil {
		f.t.Fatal(err)
	}
}

func (f *readRecoveryFixture) read(
	key, expected string,
	found bool,
) readKVResult {
	f.t.Helper()

	before := len(f.results)

	f.submit(key)
	f.process()

	if len(f.results) != before+1 {
		f.t.Fatal("expected exactly one new confirmation")
	}

	result := f.results[before]

	if result.value != expected || result.found != found {
		f.t.Fatalf(
			"key %q: got (%q, %v), want (%q, %v)",
			key, result.value, result.found,
			expected, found,
		)
	}

	if result.state.RequestID == 0 ||
		result.state.Term != f.node.Status().Term ||
		result.state.Index == 0 ||
		result.applied < result.state.Index ||
		string(result.state.Context) != key {
		f.t.Fatalf("invalid confirmation: %+v", result)
	}

	return result
}

func (f *readRecoveryFixture) saveSnapshot() (uint64, uint64) {
	f.t.Helper()

	index, data, dedup := f.store.CaptureSnapshot()

	term, found := f.node.TermAt(index)
	if !found || index == 0 {
		f.t.Fatal("invalid snapshot boundary")
	}

	saved := snapshot.NewWithDedup(
		index,
		term,
		data,
		dedup,
	)

	if err := snapshot.Save(f.snapPath, saved); err != nil {
		f.t.Fatal(err)
	}

	verified, err := snapshot.Load(f.snapPath)
	if err != nil {
		f.t.Fatal(err)
	}

	if verified.AppliedIndex != index ||
		verified.AppliedTerm != term {
		f.t.Fatal("snapshot verification failed")
	}

	return index, term
}

func TestDriverReadRecoveryFollowerRejects(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")
	f.restart()

	if f.node.Status().Role != raft.Follower {
		t.Fatal("recovered node is not a follower")
	}

	err := f.node.ReadIndex([]byte("alpha"))

	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}

	f.process()

	if len(f.results) != 0 {
		t.Fatal("recovered follower delivered a confirmation")
	}
}

func TestDriverReadRecoveryFreshElection(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")
	first := f.read("alpha", "100", true)

	f.restart()
	f.process()
	f.elect()

	second := f.read("alpha", "100", true)

	if second.state.Term <= first.state.Term {
		t.Fatal("recovered leader did not establish a fresh term")
	}

	if second.state.Index <= first.state.Index {
		t.Fatal("fresh election did not advance commit barrier")
	}
}

func TestDriverReadRecoveryUnacknowledgedDiscarded(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")
	f.submit("alpha")

	if len(f.node.Ready().ReadStates) != 1 {
		t.Fatal("expected unacknowledged confirmation")
	}

	if len(f.results) != 0 {
		t.Fatal("confirmation delivered before processing")
	}

	f.restart()

	if len(f.node.Ready().ReadStates) != 0 {
		t.Fatal("volatile confirmation survived recovery")
	}

	f.process()
	f.elect()

	if len(f.results) != 0 {
		t.Fatal("obsolete confirmation delivered after election")
	}

	f.read("alpha", "100", true)
}

func TestDriverReadRecoveryAcknowledgedNotReplayed(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")
	f.read("alpha", "100", true)

	f.restart()
	f.process()
	f.elect()

	for i := 0; i < 10; i++ {
		f.process()
	}

	if len(f.results) != 0 {
		t.Fatal("acknowledged read was replayed")
	}

	f.read("alpha", "100", true)
}

func TestDriverReadRecoveryMultipleConfirmationsDiscarded(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")
	f.put("beta", "200")

	for i := 0; i < 100; i++ {
		f.submit("alpha")
	}

	if len(f.node.Ready().ReadStates) != 100 {
		t.Fatal("incorrect pre-recovery confirmation count")
	}

	f.restart()
	f.process()
	f.elect()

	if len(f.node.Ready().ReadStates) != 0 ||
		len(f.results) != 0 {
		t.Fatal("obsolete confirmations survived recovery")
	}

	f.read("alpha", "100", true)
	f.read("beta", "200", true)
}

func TestDriverReadRecoveryWALReplay(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")
	f.put("alpha", "200")
	f.put("beta", "300")

	f.propose(kvstore.Command{
		Op:  kvstore.OpDelete,
		Key: "beta",
	})

	index := f.store.AppliedIndex()

	f.restart()
	f.process()

	if f.store.AppliedIndex() != index {
		t.Fatal("WAL replay restored incorrect applied index")
	}

	f.elect()

	f.read("alpha", "200", true)
	f.read("beta", "", false)
}

func TestDriverReadRecoverySnapshot(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")
	f.put("beta", "200")

	index, _ := f.saveSnapshot()

	f.restart()

	if f.store.AppliedIndex() != index {
		t.Fatal("snapshot applied index was not restored")
	}

	f.process()
	f.elect()

	f.read("alpha", "100", true)
	f.read("beta", "200", true)
}

func TestDriverReadRecoveryCompactedWAL(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")

	index, term := f.saveSnapshot()

	f.put("alpha", "200")
	f.put("beta", "300")

	if err := f.compact(index, term); err != nil {
		t.Fatal(err)
	}

	f.restart()

	base := f.node.SnapshotMetadata()

	if base.LastIncludedIndex != index ||
		base.LastIncludedTerm != term {
		t.Fatalf("incorrect recovered snapshot boundary: %+v", base)
	}

	f.process()
	f.elect()

	f.read("alpha", "200", true)
	f.read("beta", "300", true)
}

func TestDriverReadRecoveryDeduplication(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("counter", "10")

	command := kvstore.Command{
		Op:        kvstore.OpCompareAndSwap,
		Key:       "counter",
		Expected:  "10",
		Value:     "20",
		ClientID:  "client",
		RequestID: "cas-1",
	}

	f.propose(command)
	f.read("counter", "20", true)

	index, term := f.saveSnapshot()

	if err := f.compact(index, term); err != nil {
		t.Fatal(err)
	}

	f.restart()
	f.process()
	f.elect()

	result, found := f.store.RequestResult(
		"client", "cas-1",
	)

	if !found || !result.Applied || result.Value != "20" {
		t.Fatal("deduplication result was not recovered")
	}

	f.put("counter", "30")

	// Replaying the original CAS must not execute it again.
	f.propose(command)

	f.read("counter", "30", true)

	recovered, found := f.store.RequestResult(
		"client", "cas-1",
	)

	if !found || recovered != result {
		t.Fatal("original deduplication result changed")
	}
}

func TestDriverReadRecoveryRepeatedRestarts(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")

	previousTerm := f.node.Status().Term

	for i := 0; i < 5; i++ {
		f.submit("alpha")

		f.restart()
		f.process()
		f.elect()

		if f.node.Status().Term <= previousTerm {
			t.Fatal("term did not advance after recovery")
		}

		if len(f.results) != 0 {
			t.Fatal("obsolete read survived restart")
		}

		expected := fmt.Sprintf("value-%d", i)

		f.put("alpha", expected)
		f.read("alpha", expected, true)

		previousTerm = f.node.Status().Term
	}
}

func TestDriverReadRecoveryNoDurableReadChanges(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "100")

	index, data, dedup := f.store.CaptureSnapshot()
	status := f.node.Status()
	logBefore := f.node.DebugLog()

	for i := 0; i < 100; i++ {
		f.read("alpha", "100", true)
	}

	if !reflect.DeepEqual(status, f.node.Status()) ||
		!reflect.DeepEqual(logBefore, f.node.DebugLog()) {
		t.Fatal("read processing modified durable Raft state")
	}

	f.restart()
	f.process()

	afterIndex, afterData, afterDedup :=
		f.store.CaptureSnapshot()

	if afterIndex != index ||
		!reflect.DeepEqual(data, afterData) ||
		!reflect.DeepEqual(dedup, afterDedup) {
		t.Fatal("reads changed recovered KV state")
	}

	if f.node.Status().Commit != status.Commit ||
		f.node.Status().Term != status.Term {
		t.Fatal("reads changed recovered hard state")
	}

	if len(f.node.Ready().ReadStates) != 0 ||
		len(f.results) != 0 {
		t.Fatal("read confirmations were persisted")
	}

	f.elect()
	f.read("alpha", "100", true)
}

func TestDriverReadRecoveryNewWrites(t *testing.T) {
	f := newReadRecoveryFixture(t)

	f.put("alpha", "before-restart")

	first := f.read("alpha", "before-restart", true)

	f.restart()
	f.process()
	f.elect()

	f.put("alpha", "after-restart")

	second := f.read("alpha", "after-restart", true)

	if second.state.Index <= first.state.Index ||
		second.state.Term <= first.state.Term {
		t.Fatal("new read did not establish a fresh barrier")
	}

	f.propose(kvstore.Command{
		Op:  kvstore.OpDelete,
		Key: "alpha",
	})

	f.read("alpha", "", false)
}
