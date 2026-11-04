package raft

import "errors"

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

	// Track log replication progress for each follower.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	heartbeatTimeout int

	nextRequestID uint64
	pendingAppend map[NodeID]map[uint64]uint64
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

func (r *Raft) Propose(data []byte) error {
	if r.volatile.Role != Leader {
		return ErrNotLeader
	}

	r.appendEntry(data, EntryNormal)
	r.replicate()

	// Also handles immediate majority in a one-node cluster.
	r.maybeCommit()

	return nil
}

func (r *Raft) ReadIndex(ctx []byte) error {
	return ErrNotImplemented
}

func (r *Raft) Ready() Ready {
	return Ready{
		Messages: append([]Message(nil), r.messages...),
	}
}

func (r *Raft) Advance() {
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
		// Use the last index known to be replicated
		// on this follower, not the leader's last index.
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
