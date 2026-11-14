package raft

import (
	"bytes"
	"errors"
)

var ErrNotLeader = errors.New("node is not leader")
var ErrNotImplemented = errors.New("not implemented yet")

type Raft struct {
	id    NodeID
	peers []NodeID

	hard     HardState
	volatile VolatileState
	log      []Entry

	snapshotIndex uint64
	snapshotTerm  uint64

	votes    map[NodeID]bool
	messages []Message

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	heartbeatTimeout int

	nextRequestID uint64
	pendingAppend map[NodeID]map[uint64]uint64

	stableHard HardState
	stableLog  []Entry
}

var _ Consensus = (*Raft)(nil)

func NewRaft(
	id NodeID,
	peers []NodeID,
	electionTimeout int,
) *Raft {
	if electionTimeout <= 0 {
		panic("election timeout must be positive")
	}

	r := &Raft{
		id:               id,
		peers:            append([]NodeID(nil), peers...),
		votes:            make(map[NodeID]bool),
		nextIndex:        make(map[NodeID]uint64),
		matchIndex:       make(map[NodeID]uint64),
		pendingAppend:    make(map[NodeID]map[uint64]uint64),
		heartbeatTimeout: 2,
	}

	r.volatile.Role = Follower
	r.volatile.ElectionTimeout = electionTimeout

	return r
}

func NewRaftFromState(
	id NodeID,
	peers []NodeID,
	electionTimeout int,
	state PersistentState,
) (*Raft, error) {
	if electionTimeout <= 0 {
		return nil, errors.New("election timeout must be positive")
	}

	base := state.Snapshot.LastIncludedIndex
	baseTerm := state.Snapshot.LastIncludedTerm

	if (base == 0) != (baseTerm == 0) {
		return nil, errors.New("invalid snapshot boundary")
	}

	last := base + uint64(len(state.Log))
	if last < base {
		return nil, errors.New("recovered log index overflow")
	}

	if state.HardState.Commit < base ||
		state.HardState.Commit > last {
		return nil, errors.New(
			"recovered commit index outside log bounds",
		)
	}

	if state.HardState.Term == 0 && state.HardState.Vote != 0 {
		return nil, errors.New(
			"recovered vote exists without a term",
		)
	}

	if baseTerm > state.HardState.Term {
		return nil, errors.New(
			"snapshot term exceeds current term",
		)
	}

	previousTerm := baseTerm

	for i, entry := range state.Log {
		expected := base + uint64(i) + 1

		if entry.Index != expected {
			return nil, errors.New(
				"recovered log contains invalid index",
			)
		}

		if entry.Term == 0 || entry.Term < previousTerm {
			return nil, errors.New(
				"recovered log contains invalid term",
			)
		}

		if entry.Term > state.HardState.Term {
			return nil, errors.New(
				"recovered log term exceeds current term",
			)
		}

		previousTerm = entry.Term
	}

	r := NewRaft(id, peers, electionTimeout)

	r.snapshotIndex = base
	r.snapshotTerm = baseTerm

	r.hard = state.HardState
	r.log = cloneEntries(state.Log)

	r.stableHard = state.HardState
	r.stableLog = cloneEntries(state.Log)

	r.volatile.Role = Follower
	r.volatile.LeaderID = 0
	r.volatile.CommitIndex = state.HardState.Commit
	r.volatile.AppliedIndex = base

	return r, nil
}

func (r *Raft) Tick() {
	if r.volatile.Role == Leader {
		r.volatile.HeartbeatElapsed++

		if r.volatile.HeartbeatElapsed >= r.heartbeatTimeout {
			r.volatile.HeartbeatElapsed = 0
			r.sendHeartbeats()
		}
		return
	}

	r.volatile.ElectionElapsed++

	if r.volatile.ElectionElapsed >= r.volatile.ElectionTimeout {
		r.startElection()
	}
}

func (r *Raft) Step(msg Message) error {
	if msg.To != r.id {
		return errors.New("message addressed to another node")
	}

	if msg.Term > r.hard.Term {
		r.becomeFollower(msg.Term, 0)
	}

	switch msg.Type {
	case MsgVote:
		r.handleVoteRequest(msg)

	case MsgVoteResp:
		r.handleVoteResponse(msg)

	case MsgHeartbeat, MsgAppend:
		r.handleAppend(msg)

	case MsgAppendResp:
		r.handleAppendResponse(msg)
	}

	return nil
}

func (r *Raft) Propose(data []byte) error {
	if r.volatile.Role != Leader {
		return ErrNotLeader
	}

	r.appendEntry(data, EntryNormal)
	r.replicate()
	r.maybeCommit()

	return nil
}

func (r *Raft) ReadIndex(ctx []byte) error {
	return ErrNotImplemented
}

// Ready reports changes using absolute Raft indexes.
func (r *Raft) Ready() Ready {
	rd := Ready{
		Messages: make([]Message, len(r.messages)),
	}

	for i, msg := range r.messages {
		rd.Messages[i] = msg
		rd.Messages[i].Entries = cloneEntries(msg.Entries)
	}

	if r.hard != r.stableHard {
		hs := r.hard
		rd.HardState = &hs
	}

	common := 0

	for common < len(r.log) &&
		common < len(r.stableLog) {
		current := r.log[common]
		stable := r.stableLog[common]

		if current.Index != stable.Index ||
			current.Term != stable.Term ||
			current.Type != stable.Type ||
			!bytes.Equal(current.Data, stable.Data) {
			break
		}

		common++
	}

	// Absolute index, not slice-relative index.
	if common < len(r.stableLog) {
		rd.TruncateFrom = r.snapshotIndex +
			uint64(common) + 1
	}

	if common < len(r.log) {
		rd.Entries = cloneEntries(r.log[common:])
	}

	// Replay only entries after the applied index.
	// Snapshot-covered entries are not present in r.log.
	for _, entry := range r.log {
		if entry.Index > r.volatile.AppliedIndex &&
			entry.Index <= r.hard.Commit {
			rd.CommittedEntries = append(
				rd.CommittedEntries,
				cloneEntries([]Entry{entry})...,
			)
		}
	}

	return rd
}

func (r *Raft) Advance() {
	r.stableHard = r.hard
	r.stableLog = cloneEntries(r.log)

	r.volatile.AppliedIndex = r.hard.Commit
	r.messages = nil
}

func (r *Raft) Status() Status {
	return Status{
		ID:       r.id,
		Role:     r.volatile.Role,
		Term:     r.hard.Term,
		LeaderID: r.volatile.LeaderID,
		Commit:   r.hard.Commit,
	}
}

func (r *Raft) majority() int {
	return (len(r.peers)+1)/2 + 1
}

func (r *Raft) sendHeartbeats() {
	for _, peer := range r.peers {
		matchIndex := r.matchIndex[peer]

		r.messages = append(r.messages, Message{
			Type:    MsgHeartbeat,
			From:    r.id,
			To:      peer,
			Term:    r.hard.Term,
			Index:   matchIndex,
			LogTerm: r.termAt(matchIndex),
			Commit:  r.hard.Commit,
		})
	}
}

func (r *Raft) lastIndex() uint64 {
	if len(r.log) == 0 {
		return r.snapshotIndex
	}

	return r.log[len(r.log)-1].Index
}

func (r *Raft) firstIndex() uint64 {
	return r.snapshotIndex + 1
}

func (r *Raft) logOffset(index uint64) (int, bool) {
	if index <= r.snapshotIndex ||
		index > r.lastIndex() {
		return 0, false
	}

	return int(index - r.snapshotIndex - 1), true
}

func (r *Raft) entryAt(index uint64) (Entry, bool) {
	offset, ok := r.logOffset(index)
	if !ok {
		return Entry{}, false
	}

	return r.log[offset], true
}

// SnapshotMetadata returns the recovered snapshot boundary.
func (r *Raft) SnapshotMetadata() SnapshotMetadata {
	return SnapshotMetadata{
		LastIncludedIndex: r.snapshotIndex,
		LastIncludedTerm:  r.snapshotTerm,
	}
}

// TermAt returns the term of an available log index
// or the snapshot boundary. Older terms are unavailable.
func (r *Raft) TermAt(index uint64) (uint64, bool) {
	if index == r.snapshotIndex {
		if index == 0 {
			return 0, true
		}
		return r.snapshotTerm, true
	}

	entry, ok := r.entryAt(index)
	if !ok {
		return 0, false
	}

	return entry.Term, true
}

func cloneEntries(entries []Entry) []Entry {
	if len(entries) == 0 {
		return nil
	}

	copied := make([]Entry, len(entries))

	for i, entry := range entries {
		copied[i] = entry
		copied[i].Data = append([]byte(nil), entry.Data...)
	}

	return copied
}
