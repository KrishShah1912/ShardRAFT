package observability

import (
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

type Event struct {
	ID        uint64         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	NodeID    raft.NodeID    `json:"node_id"`
	Category  string         `json:"category"`
	Type      string         `json:"type"`
	Term      uint64         `json:"term,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

const (
	CategoryNode      = "node"
	CategoryRaft      = "raft"
	CategoryKV        = "kv"
	CategoryTransport = "transport"
	CategoryStorage   = "storage"
	CategoryFault     = "fault"
)

const (
	EventNodeStarted   = "node.started"
	EventNodeStopping  = "node.stopping"
	EventNodeStopped   = "node.stopped"
	EventNodeRecovered = "node.recovered"

	EventRaftRoleChanged         = "raft.role.changed"
	EventRaftTermChanged         = "raft.term.changed"
	EventRaftElectionStarted     = "raft.election.started"
	EventRaftVoteRequested       = "raft.vote.requested"
	EventRaftVoteGranted         = "raft.vote.granted"
	EventRaftVoteRejected        = "raft.vote.rejected"
	EventRaftLeaderElected       = "raft.leader.elected"
	EventRaftAppendSent          = "raft.append.sent"
	EventRaftAppendReceived      = "raft.append.received"
	EventRaftAppendAccepted      = "raft.append.accepted"
	EventRaftAppendRejected      = "raft.append.rejected"
	EventRaftReplicationProgress = "raft.replication.progress"
	EventRaftCommitAdvanced      = "raft.commit.advanced"
	EventRaftHeartbeatSent       = "raft.heartbeat.sent"
	EventRaftHeartbeatReceived   = "raft.heartbeat.received"
	EventRaftEntryAppended       = "raft.entry.appended"
	EventRaftLogTruncated        = "raft.log.truncated"
	EventRaftNoopAppended        = "raft.noop.appended"

	EventKVRequestReceived     = "kv.request.received"
	EventKVRequestProposed     = "kv.request.proposed"
	EventKVRequestCommitted    = "kv.request.committed"
	EventKVEntryApplied        = "kv.entry.applied"
	EventKVPut                 = "kv.put"
	EventKVDelete              = "kv.delete"
	EventKVCASSuccess          = "kv.cas.success"
	EventKVCASFailed           = "kv.cas.failed"
	EventKVRequestDeduplicated = "kv.request.deduplicated"

	EventTransportQueued       = "transport.message.queued"
	EventTransportSent         = "transport.message.sent"
	EventTransportDelivered    = "transport.message.delivered"
	EventTransportRetry        = "transport.retry"
	EventTransportBackpressure = "transport.backpressure"
	EventTransportFailure      = "transport.failure"

	EventWALWrite          = "wal.write"
	EventWALSegmentCreated = "wal.segment.created"
	EventWALSegmentRotated = "wal.segment.rotated"
	EventSnapshotStarted   = "snapshot.started"
	EventSnapshotCreated   = "snapshot.created"
	EventSnapshotLoaded    = "snapshot.loaded"
	EventStorageRecovered  = "storage.recovered"
)
