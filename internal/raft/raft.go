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

	votes    map[NodeID]bool
	messages []Message

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	heartbeatTimeout int

	nextRequestID uint64
	pendingAppend map[NodeID]map[uint64]uint64

	// State acknowledged by Advance().
	stableHard HardState
	stableLog  []Entry
}

var _ Consensus = (*Raft)(nil)

// NewRaft creates a Raft node with a deterministic timeout.
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

// NewRaftFromState restores a Raft node from persisted state.
//
// A recovered node always starts as a follower.
// Persisted entries and HardState are marked stable.
//
// Committed entries remain available for replay into
// the application state machine.
func NewRaftFromState(
	id NodeID,
	peers []NodeID,
	electionTimeout int,
	state PersistentState,
) (*Raft, error) {
	// Validate the recovered commit index.
	if state.HardState.Commit > uint64(len(state.Log)) {
		return nil, errors.New(
			"recovered commit index exceeds log length",
		)
	}

	// A vote cannot exist without an election term.
	if state.HardState.Term == 0 &&
		state.HardState.Vote != 0 {
		return nil, errors.New(
			"recovered vote exists without a term",
		)
	}

	// Validate recovered log indexes and terms.
	for i, entry := range state.Log {
		expectedIndex := uint64(i + 1)

		if entry.Index != expectedIndex {
			return nil, errors.New(
				"recovered log contains invalid index",
			)
		}

		if entry.Term == 0 {
			return nil, errors.New(
				"recovered log contains zero term",
			)
		}

		if i > 0 && entry.Term < state.Log[i-1].Term {
			return nil, errors.New(
				"recovered log contains decreasing terms",
			)
		}

		if entry.Term > state.HardState.Term {
			return nil, errors.New(
				"recovered log term exceeds current term",
			)
		}
	}

	// Construct a fresh Raft node.
	r := NewRaft(id, peers, electionTimeout)

	// Restore persistent metadata.
	r.hard = state.HardState

	// Deep-copy recovered log entries.
	r.log = cloneEntries(state.Log)

	// Recovered state is already persisted.
	r.stableHard = state.HardState
	r.stableLog = cloneEntries(state.Log)

	// Always restart as follower.
	r.volatile.Role = Follower
	r.volatile.LeaderID = 0

	// Restore the commit index.
	r.volatile.CommitIndex = state.HardState.Commit

	// Replay committed entries before normal operation.
	r.volatile.AppliedIndex = 0

	// Reset election and heartbeat timers.
	r.volatile.ElectionElapsed = 0
	r.volatile.HeartbeatElapsed = 0

	return r, nil
}

// Tick advances simulated time by one logical tick.
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

// Step processes an incoming Raft message.
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

// Propose appends a command to the leader's log.
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

// Ready exposes pending messages and persistence work.
func (r *Raft) Ready() Ready {
	rd := Ready{
		Messages: make([]Message, len(r.messages)),
	}

	// Copy outgoing messages.
	for i, msg := range r.messages {
		rd.Messages[i] = msg
		rd.Messages[i].Entries = cloneEntries(msg.Entries)
	}

	// Expose changed persistent metadata.
	if r.hard != r.stableHard {
		hs := r.hard
		rd.HardState = &hs
	}

	// Determine the common prefix of the current log
	// and the last acknowledged log.
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

	// Detect a replaced or removed persisted suffix.
	if common < len(r.stableLog) {
		rd.TruncateFrom = uint64(common) + 1
	}

	// Include new or replacement entries.
	if common < len(r.log) {
		rd.Entries = cloneEntries(r.log[common:])
	}

	// Expose committed entries that have not yet
	// been acknowledged as applied.
	if r.volatile.AppliedIndex < r.hard.Commit {
		for _, entry := range r.log {
			if entry.Index > r.volatile.AppliedIndex &&
				entry.Index <= r.hard.Commit {
				rd.CommittedEntries = append(
					rd.CommittedEntries,
					cloneEntries([]Entry{entry})...,
				)
			}
		}
	}

	return rd
}

// Advance acknowledges all current Ready work.
//
// A durable driver must persist the required state
// and apply committed entries successfully before
// calling Advance().
func (r *Raft) Advance() {
	r.stableHard = r.hard
	r.stableLog = cloneEntries(r.log)

	r.volatile.AppliedIndex = r.hard.Commit
	r.messages = nil
}

// Status returns the node's consensus status.
func (r *Raft) Status() Status {
	return Status{
		ID:       r.id,
		Role:     r.volatile.Role,
		Term:     r.hard.Term,
		LeaderID: r.volatile.LeaderID,
		Commit:   r.hard.Commit,
	}
}

// majority returns the quorum size.
func (r *Raft) majority() int {
	return (len(r.peers)+1)/2 + 1
}

// sendHeartbeats sends heartbeats to followers.
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

// cloneEntries deep-copies entries and their payloads.
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
