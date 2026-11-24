package raft

// Event represents an observable Raft state transition.
//
// Events are informational only. They must never be required for
// consensus correctness or progress.
type Event struct {
	Type string
	Data map[string]any
}

// EventHandler receives observable Raft events.
//
// Handlers must return quickly. Raft does not depend on the handler
// for correctness.
type EventHandler func(Event)

const (
	EventRoleChanged         = "raft.role.changed"
	EventTermChanged         = "raft.term.changed"
	EventElectionStarted     = "raft.election.started"
	EventVoteRequested       = "raft.vote.requested"
	EventVoteGranted         = "raft.vote.granted"
	EventVoteRejected        = "raft.vote.rejected"
	EventLeaderElected       = "raft.leader.elected"
	EventEntryAppended       = "raft.entry.appended"
	EventNoopAppended        = "raft.noop.appended"
	EventAppendSent          = "raft.append.sent"
	EventAppendReceived      = "raft.append.received"
	EventAppendAccepted      = "raft.append.accepted"
	EventAppendRejected      = "raft.append.rejected"
	EventReplicationProgress = "raft.replication.progress"
	EventCommitAdvanced      = "raft.commit.advanced"

	EventHeartbeatSent     = "raft.heartbeat.sent"
	EventHeartbeatReceived = "raft.heartbeat.received"
	EventHeartbeatAccepted = "raft.heartbeat.accepted"
)

func (r *Raft) SetEventHandler(handler EventHandler) {
	r.eventHandler = handler
}

func (r *Raft) emit(eventType string, data map[string]any) {
	if r.eventHandler == nil {
		return
	}

	copied := make(map[string]any, len(data))
	for key, value := range data {
		copied[key] = value
	}

	r.eventHandler(Event{
		Type: eventType,
		Data: copied,
	})
}
