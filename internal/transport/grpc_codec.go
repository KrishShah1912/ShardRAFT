package transport

import (
	"bytes"
	"fmt"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

// EncodeRaftMessage converts an internal Raft message
// into its protobuf representation.
func EncodeRaftMessage(
	message raft.Message,
) (*shardraftpb.RaftMessage, error) {
	if message.Type > raft.MsgHeartbeatResp {
		return nil, fmt.Errorf(
			"invalid Raft message type: %d",
			message.Type,
		)
	}

	result := &shardraftpb.RaftMessage{
		Type:      shardraftpb.MessageType(message.Type),
		From:      uint64(message.From),
		To:        uint64(message.To),
		Term:      message.Term,
		LogTerm:   message.LogTerm,
		Index:     message.Index,
		Commit:    message.Commit,
		RequestId: message.RequestID,
		Reject:    message.Reject,
	}

	if len(message.Entries) > 0 {
		result.Entries = make(
			[]*shardraftpb.LogEntry,
			0,
			len(message.Entries),
		)
	}

	for _, entry := range message.Entries {
		if entry.Type > raft.EntryNoop {
			return nil, fmt.Errorf(
				"invalid Raft entry type: %d",
				entry.Type,
			)
		}

		result.Entries = append(
			result.Entries,
			&shardraftpb.LogEntry{
				Term:  entry.Term,
				Index: entry.Index,
				Type:  shardraftpb.EntryType(entry.Type),
				Data:  bytes.Clone(entry.Data),
			},
		)
	}

	return result, nil
}

// DecodeRaftMessage converts a protobuf message
// into the internal Raft representation.
func DecodeRaftMessage(
	message *shardraftpb.RaftMessage,
) (raft.Message, error) {
	if message == nil {
		return raft.Message{}, fmt.Errorf(
			"Raft message cannot be nil",
		)
	}

	if message.Type < shardraftpb.MessageType_MSG_VOTE ||
		message.Type > shardraftpb.MessageType_MSG_HEARTBEAT_RESP {
		return raft.Message{}, fmt.Errorf(
			"invalid protobuf message type: %d",
			message.Type,
		)
	}

	result := raft.Message{
		Type:      raft.MessageType(message.Type),
		From:      raft.NodeID(message.From),
		To:        raft.NodeID(message.To),
		Term:      message.Term,
		LogTerm:   message.LogTerm,
		Index:     message.Index,
		Commit:    message.Commit,
		RequestID: message.RequestId,
		Reject:    message.Reject,
	}

	if len(message.Entries) > 0 {
		result.Entries = make(
			[]raft.Entry,
			0,
			len(message.Entries),
		)
	}

	for _, entry := range message.Entries {
		if entry == nil {
			return raft.Message{}, fmt.Errorf(
				"Raft entry cannot be nil",
			)
		}

		if entry.Type < shardraftpb.EntryType_ENTRY_NORMAL ||
			entry.Type > shardraftpb.EntryType_ENTRY_NOOP {
			return raft.Message{}, fmt.Errorf(
				"invalid protobuf entry type: %d",
				entry.Type,
			)
		}

		result.Entries = append(
			result.Entries,
			raft.Entry{
				Term:  entry.Term,
				Index: entry.Index,
				Type:  raft.EntryType(entry.Type),
				Data:  bytes.Clone(entry.Data),
			},
		)
	}

	return result, nil
}
