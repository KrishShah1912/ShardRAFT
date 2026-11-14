package raft

type NodeID uint64

// Role represents the current Raft role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

// EntryType distinguishes commands from internal entries.
type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryNoop
)

// Entry represents one replicated log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// MessageType identifies Raft protocol messages.
type MessageType uint8

const (
	MsgVote MessageType = iota
	MsgVoteResp
	MsgAppend
	MsgAppendResp
	MsgHeartbeat
	MsgHeartbeatResp
)

// Message represents communication between nodes.
type Message struct {
	Type MessageType

	From NodeID
	To   NodeID

	Term    uint64
	LogTerm uint64
	Index   uint64
	Commit  uint64

	RequestID uint64

	Entries []Entry
	Reject  bool
}

// HardState is the persistent Raft metadata.
type HardState struct {
	Term   uint64
	Vote   NodeID
	Commit uint64
}

// PersistentState survives restarts.
type PersistentState struct {
	HardState HardState
	Log       []Entry
}

// VolatileState is reconstructed after restart.
type VolatileState struct {
	Role Role

	LeaderID NodeID

	CommitIndex  uint64
	AppliedIndex uint64

	ElectionElapsed  int
	HeartbeatElapsed int
	ElectionTimeout  int
}

// Ready contains pending work produced by Raft.
type Ready struct {
	Messages         []Message
	Entries          []Entry
	CommittedEntries []Entry
	HardState        *HardState

	// First index of a persisted suffix that must be removed.
	// Zero means no truncation is required.
	TruncateFrom uint64
}

// Status exposes the node's current state.
type Status struct {
	ID       NodeID
	Role     Role
	Term     uint64
	LeaderID NodeID
	Commit   uint64
}
