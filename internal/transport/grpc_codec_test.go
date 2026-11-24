package transport

import (
	"reflect"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/protobuf/proto"
)

func TestGRPCCodecRoundTrip(t *testing.T) {
	original := raft.Message{
		Type:      raft.MsgAppend,
		From:      1,
		To:        2,
		Term:      7,
		LogTerm:   6,
		Index:     10,
		Commit:    9,
		RequestID: 12345,
		Reject:    true,
		Entries: []raft.Entry{
			{
				Term:  7,
				Index: 11,
				Type:  raft.EntryNormal,
				Data:  []byte("test-command"),
			},
			{
				Term:  7,
				Index: 12,
				Type:  raft.EntryNoop,
			},
		},
	}

	encoded, err := EncodeRaftMessage(original)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRaftMessage(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf(
			"round trip mismatch:\nwant: %+v\ngot: %+v",
			original,
			decoded,
		)
	}
}

func TestGRPCCodecAllMessageTypes(t *testing.T) {
	types := []raft.MessageType{
		raft.MsgVote,
		raft.MsgVoteResp,
		raft.MsgAppend,
		raft.MsgAppendResp,
		raft.MsgHeartbeat,
		raft.MsgHeartbeatResp,
	}

	for _, messageType := range types {
		original := raft.Message{
			Type:      messageType,
			From:      1,
			To:        2,
			Term:      3,
			RequestID: 99,
		}

		encoded, err := EncodeRaftMessage(original)
		if err != nil {
			t.Fatal(err)
		}

		decoded, err := DecodeRaftMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(original, decoded) {
			t.Fatalf(
				"message type %d mismatch",
				messageType,
			)
		}
	}
}

func TestGRPCCodecEmptyEntries(t *testing.T) {
	original := raft.Message{
		Type: raft.MsgHeartbeat,
		From: 1,
		To:   2,
		Term: 5,
	}

	encoded, err := EncodeRaftMessage(original)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRaftMessage(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if len(decoded.Entries) != 0 {
		t.Fatal("expected empty entries")
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Fatal("empty-entry round trip mismatch")
	}
}

func TestGRPCCodecRejectNilMessage(t *testing.T) {
	if _, err := DecodeRaftMessage(nil); err == nil {
		t.Fatal("expected nil message rejection")
	}
}

func TestGRPCCodecRejectInvalidMessageType(t *testing.T) {
	if _, err := EncodeRaftMessage(
		raft.Message{
			Type: raft.MessageType(255),
		},
	); err == nil {
		t.Fatal("expected invalid internal type rejection")
	}

	for _, invalid := range []int32{-1, 6, 256} {
		if _, err := DecodeRaftMessage(
			&shardraftpb.RaftMessage{
				Type: shardraftpb.MessageType(invalid),
			},
		); err == nil {
			t.Fatalf(
				"expected rejection for type %d",
				invalid,
			)
		}
	}
}

func TestGRPCCodecRejectInvalidEntryType(t *testing.T) {
	if _, err := EncodeRaftMessage(
		raft.Message{
			Type: raft.MsgAppend,
			Entries: []raft.Entry{
				{Type: raft.EntryType(255)},
			},
		},
	); err == nil {
		t.Fatal("expected invalid internal entry rejection")
	}

	for _, invalid := range []int32{-1, 2, 256} {
		if _, err := DecodeRaftMessage(
			&shardraftpb.RaftMessage{
				Entries: []*shardraftpb.LogEntry{
					{
						Type: shardraftpb.EntryType(invalid),
					},
				},
			},
		); err == nil {
			t.Fatalf(
				"expected rejection for entry type %d",
				invalid,
			)
		}
	}
}

func TestGRPCCodecRejectNilEntry(t *testing.T) {
	message := &shardraftpb.RaftMessage{
		Entries: []*shardraftpb.LogEntry{nil},
	}

	if _, err := DecodeRaftMessage(message); err == nil {
		t.Fatal("expected nil entry rejection")
	}
}

func TestGRPCCodecDataIsolation(t *testing.T) {
	original := raft.Message{
		Type: raft.MsgAppend,
		Entries: []raft.Entry{
			{
				Type: raft.EntryNormal,
				Data: []byte("original"),
			},
		},
	}

	encoded, err := EncodeRaftMessage(original)
	if err != nil {
		t.Fatal(err)
	}

	original.Entries[0].Data[0] = 'X'

	if string(encoded.Entries[0].Data) != "original" {
		t.Fatal("encoded data shares original memory")
	}

	decoded, err := DecodeRaftMessage(encoded)
	if err != nil {
		t.Fatal(err)
	}

	encoded.Entries[0].Data[0] = 'Y'

	if string(decoded.Entries[0].Data) != "original" {
		t.Fatal("decoded data shares protobuf memory")
	}
}

func TestGRPCCodecWireRoundTrip(t *testing.T) {
	original := raft.Message{
		Type:      raft.MsgAppend,
		From:      1,
		To:        3,
		Term:      8,
		LogTerm:   7,
		Index:     20,
		Commit:    19,
		RequestID: 54321,
		Entries: []raft.Entry{
			{
				Term:  8,
				Index: 21,
				Type:  raft.EntryNormal,
				Data:  []byte{0, 1, 2, 255},
			},
		},
	}

	encoded, err := EncodeRaftMessage(original)
	if err != nil {
		t.Fatal(err)
	}

	wire, err := proto.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}

	var received shardraftpb.RaftMessage

	if err := proto.Unmarshal(wire, &received); err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRaftMessage(&received)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Fatal("protobuf wire round trip mismatch")
	}
}
