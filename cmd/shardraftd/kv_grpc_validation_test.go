package main

import (
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

func requireKVValidationCode(
	t *testing.T,
	failure *shardraftpb.RoutingError,
	expected shardraftpb.ErrorCode,
) {
	t.Helper()

	if expected == shardraftpb.ErrorCode_ERROR_CODE_OK {
		if failure != nil {
			t.Fatalf("unexpected error: %v", failure)
		}
		return
	}

	if failure == nil {
		t.Fatalf("expected error code %v", expected)
	}

	if failure.Code != expected {
		t.Fatalf(
			"got %v, want %v",
			failure.Code,
			expected,
		)
	}

	if failure.Message == "" {
		t.Fatal("missing error message")
	}
}

func TestKVGRPCValidationHeader(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	ok := shardraftpb.ErrorCode_ERROR_CODE_OK
	invalid := shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST
	wrong := shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD

	tests := []struct {
		name     string
		header   *shardraftpb.RequestHeader
		mutation bool
		expected shardraftpb.ErrorCode
	}{
		{"NilHeader", nil, false, invalid},
		{"ZeroShard", &shardraftpb.RequestHeader{}, false, invalid},
		{"WrongShard", &shardraftpb.RequestHeader{ShardId: 2}, false, wrong},
		{"UnspecifiedEpoch", &shardraftpb.RequestHeader{ShardId: 1}, false, ok},
		{"CurrentEpoch", &shardraftpb.RequestHeader{ShardId: 1, Epoch: 1}, false, ok},
		{"WrongEpoch", &shardraftpb.RequestHeader{ShardId: 1, Epoch: 2}, false, wrong},
		{"ReadWithoutIDs", &shardraftpb.RequestHeader{ShardId: 1}, false, ok},
		{"ReadWithIDs", &shardraftpb.RequestHeader{ShardId: 1, ClientId: "c", RequestId: "r"}, false, ok},
		{"ClientOnly", &shardraftpb.RequestHeader{ShardId: 1, ClientId: "c"}, false, invalid},
		{"RequestOnly", &shardraftpb.RequestHeader{ShardId: 1, RequestId: "r"}, false, invalid},
		{"MutationWithoutIDs", &shardraftpb.RequestHeader{ShardId: 1}, true, invalid},
		{"MutationWithIDs", &shardraftpb.RequestHeader{ShardId: 1, ClientId: "c", RequestId: "r"}, true, ok},
		{"MutationClientOnly", &shardraftpb.RequestHeader{ShardId: 1, ClientId: "c"}, true, invalid},
		{"MutationRequestOnly", &shardraftpb.RequestHeader{ShardId: 1, RequestId: "r"}, true, invalid},
		{"InvalidUTF8", &shardraftpb.RequestHeader{ShardId: 1, ClientId: "\xff", RequestId: "r"}, true, invalid},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireKVValidationCode(
				t,
				server.validateHeader(tc.header, tc.mutation),
				tc.expected,
			)
		})
	}
}

func TestKVGRPCValidationGet(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	ok := shardraftpb.ErrorCode_ERROR_CODE_OK
	invalid := shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST
	wrong := shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD

	header := &shardraftpb.RequestHeader{ShardId: 1}

	tests := []struct {
		name     string
		request  *shardraftpb.GetRequest
		expected shardraftpb.ErrorCode
	}{
		{"NilRequest", nil, invalid},
		{"MissingHeader", &shardraftpb.GetRequest{Key: "alpha"}, invalid},
		{"EmptyKey", &shardraftpb.GetRequest{Header: header}, invalid},
		{"InvalidUTF8", &shardraftpb.GetRequest{Header: header, Key: "\xff"}, invalid},
		{"DefaultLinearizable", &shardraftpb.GetRequest{Header: header, Key: "alpha"}, ok},
		{"ExplicitLinearizable", &shardraftpb.GetRequest{Header: header, Key: "alpha", ReadMode: shardraftpb.ReadMode_READ_MODE_LINEARIZABLE}, ok},
		{"ExplicitLocal", &shardraftpb.GetRequest{Header: header, Key: "alpha", ReadMode: shardraftpb.ReadMode_READ_MODE_LOCAL}, ok},
		{"UnknownMode", &shardraftpb.GetRequest{Header: header, Key: "alpha", ReadMode: shardraftpb.ReadMode(99)}, invalid},
		{"NegativeMode", &shardraftpb.GetRequest{Header: header, Key: "alpha", ReadMode: shardraftpb.ReadMode(-1)}, invalid},
		{"WrongShard", &shardraftpb.GetRequest{Header: &shardraftpb.RequestHeader{ShardId: 2}, Key: "alpha"}, wrong},
		{"Oversized", &shardraftpb.GetRequest{Header: header, Key: strings.Repeat("a", kvMaxRequestSize+1)}, invalid},
		{"UnicodeKey", &shardraftpb.GetRequest{Header: header, Key: "hello世界"}, ok},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireKVValidationCode(
				t,
				server.validateGetRequest(tc.request),
				tc.expected,
			)
		})
	}
}

func TestKVGRPCValidationMutation(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	ok := shardraftpb.ErrorCode_ERROR_CODE_OK
	invalid := shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST

	tests := []struct {
		name     string
		command  kvstore.Command
		expected shardraftpb.ErrorCode
	}{
		{"Put", kvstore.Command{Op: kvstore.OpPut, Key: "alpha", Value: "100", ClientID: "c", RequestID: "r"}, ok},
		{"EmptyValue", kvstore.Command{Op: kvstore.OpPut, Key: "alpha", ClientID: "c", RequestID: "r"}, ok},
		{"Delete", kvstore.Command{Op: kvstore.OpDelete, Key: "alpha", ClientID: "c", RequestID: "r"}, ok},
		{"CAS", kvstore.Command{Op: kvstore.OpCompareAndSwap, Key: "alpha", Expected: "100", Value: "200", ClientID: "c", RequestID: "r"}, ok},
		{"CASEmptyValues", kvstore.Command{Op: kvstore.OpCompareAndSwap, Key: "alpha", ClientID: "c", RequestID: "r"}, ok},
		{"EmptyKey", kvstore.Command{Op: kvstore.OpPut, ClientID: "c", RequestID: "r"}, invalid},
		{"MissingIDs", kvstore.Command{Op: kvstore.OpPut, Key: "alpha"}, invalid},
		{"MissingClient", kvstore.Command{Op: kvstore.OpPut, Key: "alpha", RequestID: "r"}, invalid},
		{"MissingRequest", kvstore.Command{Op: kvstore.OpPut, Key: "alpha", ClientID: "c"}, invalid},
		{"UnknownOperation", kvstore.Command{Key: "alpha", ClientID: "c", RequestID: "r"}, invalid},
		{"InvalidValue", kvstore.Command{Op: kvstore.OpPut, Key: "alpha", Value: "\xff", ClientID: "c", RequestID: "r"}, invalid},
		{"InvalidExpected", kvstore.Command{Op: kvstore.OpCompareAndSwap, Key: "alpha", Expected: "\xff", ClientID: "c", RequestID: "r"}, invalid},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireKVValidationCode(
				t,
				server.validateMutationCommand(tc.command),
				tc.expected,
			)
		})
	}
}

func TestKVGRPCValidationRoutingMetadata(t *testing.T) {
	server := newKVGRPCServer(
		1,
		nil,
		map[raft.NodeID]string{
			2: "127.0.0.1:19092",
		},
	)

	failure := server.routingError(
		shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
		"not leader",
		raft.Status{
			ID:       1,
			Role:     raft.Follower,
			Term:     7,
			LeaderID: 2,
		},
	)

	if failure.LeaderId != 2 ||
		failure.LeaderAddress != "127.0.0.1:19092" ||
		failure.Term != 7 ||
		failure.ShardId != 1 ||
		failure.Epoch != 1 {
		t.Fatalf("incorrect routing metadata: %v", failure)
	}
}

func TestKVGRPCValidationUnknownLeader(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	failure := server.routingError(
		shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE,
		"leader unavailable",
		raft.Status{Term: 3},
	)

	if failure.LeaderId != 0 ||
		failure.LeaderAddress != "" ||
		failure.Term != 3 {
		t.Fatalf("incorrect unknown leader metadata: %v", failure)
	}
}

func TestKVGRPCValidationMissingAddress(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	failure := server.routingError(
		shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
		"not leader",
		raft.Status{LeaderID: 2, Term: 3},
	)

	if failure.LeaderId != 2 ||
		failure.LeaderAddress != "" {
		t.Fatalf("incorrect leader metadata: %v", failure)
	}
}

func TestKVGRPCValidationAddressIsolation(t *testing.T) {
	addresses := map[raft.NodeID]string{
		2: "127.0.0.1:19092",
	}

	server := newKVGRPCServer(1, nil, addresses)

	addresses[2] = "changed"
	delete(addresses, 2)

	failure := server.routingError(
		shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
		"not leader",
		raft.Status{LeaderID: 2},
	)

	if failure.LeaderAddress != "127.0.0.1:19092" {
		t.Fatalf("server address map was mutated: %v", failure)
	}
}

func TestKVGRPCValidationRequestSize(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	invalid := shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST
	ok := shardraftpb.ErrorCode_ERROR_CODE_OK

	requireKVValidationCode(
		t,
		server.validateRequestSize(nil),
		invalid,
	)

	var typedNil *shardraftpb.PutRequest

	requireKVValidationCode(
		t,
		server.validateRequestSize(typedNil),
		invalid,
	)

	requireKVValidationCode(
		t,
		server.validateRequestSize(
			&shardraftpb.PutRequest{Value: "100"},
		),
		ok,
	)

	requireKVValidationCode(
		t,
		server.validateRequestSize(
			&shardraftpb.PutRequest{
				Value: strings.Repeat("a", kvMaxRequestSize+1),
			},
		),
		invalid,
	)
}
