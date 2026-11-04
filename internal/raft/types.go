package raft

type NodeID uint64

type MessageType uint8

const (
	MsgVote MessageType = iota
	MsgVoteResp
	MsgAppend
	MsgAppendResp
	MsgHeartbeat
	MsgHeartbeatResp
)

type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type Message struct {
	Type    MessageType
	From    NodeID
	To      NodeID
	Term    uint64
	LogTerm uint64
	Index   uint64
	Commit  uint64
	Entries []Entry
	Reject  bool
}

type HardState struct {
	Term   uint64
	Vote   NodeID
	Commit uint64
}

type Ready struct {
	Messages         []Message
	Entries          []Entry
	CommittedEntries []Entry
	HardState        *HardState
}

type Status struct {
	ID       NodeID
	Term     uint64
	LeaderID NodeID
	Commit   uint64
}
