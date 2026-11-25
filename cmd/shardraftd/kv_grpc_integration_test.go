package main

import (
	"context"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/raft"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func kvWireHeader(requestID string) *shardraftpb.RequestHeader {
	return &shardraftpb.RequestHeader{
		ShardId:   defaultShardID,
		Epoch:     1,
		ClientId:  "kv-wire-client",
		RequestId: requestID,
	}
}

func kvWireContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	t.Cleanup(cancel)
	return ctx
}

func kvWireRequireMutation(
	t *testing.T,
	response *shardraftpb.MutationResponse,
	err error,
	applied bool,
) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}

	if response == nil ||
		response.Error != nil ||
		response.Applied != applied {
		t.Fatalf(
			"unexpected mutation response: %v, want applied=%v",
			response, applied,
		)
	}
}

func kvWireRequireValue(
	t *testing.T,
	client shardraftpb.KVServiceClient,
	key string,
	value string,
) {
	t.Helper()

	response, err := client.Get(
		kvWireContext(t),
		&shardraftpb.GetRequest{
			Header: &shardraftpb.RequestHeader{
				ShardId: defaultShardID,
				Epoch:   1,
			},
			Key: key,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if response == nil ||
		response.Error != nil ||
		!response.Found ||
		response.Value != value {
		t.Fatalf(
			"unexpected Get response: %v, want value=%q",
			response, value,
		)
	}
}

func TestKVGRPCWireServiceRegistration(t *testing.T) {
	f := newAdminWireFixture(t)

	services := f.server.server.GetServiceInfo()

	for _, name := range []string{
		"shardraft.v1.RaftService",
		"shardraft.v1.AdminService",
		"shardraft.v1.KVService",
	} {
		if _, found := services[name]; !found {
			t.Fatalf("missing service: %s", name)
		}
	}

	kv := services["shardraft.v1.KVService"]

	methods := make(map[string]bool)

	for _, method := range kv.Methods {
		methods[method.Name] = true

		if method.IsClientStream || method.IsServerStream {
			t.Fatalf(
				"unexpected streaming method: %s",
				method.Name,
			)
		}
	}

	if len(methods) != 4 ||
		!methods["Get"] ||
		!methods["Put"] ||
		!methods["Delete"] ||
		!methods["CompareAndSwap"] {
		t.Fatalf("unexpected KV methods: %v", methods)
	}

	t.Log("Raft, Admin and KV services share one listener")
}

func TestKVGRPCWireCRUD(t *testing.T) {
	f := newAdminWireFixture(t)
	electAdminWireLeader(t, f.node)

	client := shardraftpb.NewKVServiceClient(f.conn)
	ctx := kvWireContext(t)

	put, err := client.Put(ctx, &shardraftpb.PutRequest{
		Header: kvWireHeader("put-1"),
		Key:    "wire-key",
		Value:  "initial-value",
	})

	kvWireRequireMutation(t, put, err, true)

	kvWireRequireValue(
		t, client, "wire-key", "initial-value",
	)

	cas, err := client.CompareAndSwap(
		ctx,
		&shardraftpb.CompareAndSwapRequest{
			Header:   kvWireHeader("cas-1"),
			Key:      "wire-key",
			Expected: "initial-value",
			Value:    "updated-value",
		},
	)

	kvWireRequireMutation(t, cas, err, true)

	kvWireRequireValue(
		t, client, "wire-key", "updated-value",
	)

	failedCAS, err := client.CompareAndSwap(
		ctx,
		&shardraftpb.CompareAndSwapRequest{
			Header:   kvWireHeader("cas-2"),
			Key:      "wire-key",
			Expected: "incorrect-value",
			Value:    "must-not-apply",
		},
	)

	kvWireRequireMutation(t, failedCAS, err, false)

	kvWireRequireValue(
		t, client, "wire-key", "updated-value",
	)

	deleted, err := client.Delete(
		ctx,
		&shardraftpb.DeleteRequest{
			Header: kvWireHeader("delete-1"),
			Key:    "wire-key",
		},
	)

	kvWireRequireMutation(t, deleted, err, true)

	missing, err := client.Get(
		ctx,
		&shardraftpb.GetRequest{
			Header: kvWireHeader("get-missing"),
			Key:    "wire-key",
		},
	)

	if err != nil ||
		missing == nil ||
		missing.Found ||
		missing.Error == nil ||
		missing.Error.Code !=
			shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND {
		t.Fatalf(
			"unexpected missing response=%v err=%v",
			missing, err,
		)
	}

	deletedAgain, err := client.Delete(
		ctx,
		&shardraftpb.DeleteRequest{
			Header: kvWireHeader("delete-2"),
			Key:    "wire-key",
		},
	)

	kvWireRequireMutation(t, deletedAgain, err, false)

	t.Log("Put, linearizable Get, CAS and Delete verified over gRPC")
}

func TestKVGRPCWireEmptyAndUnicodeValues(t *testing.T) {
	f := newAdminWireFixture(t)
	electAdminWireLeader(t, f.node)

	client := shardraftpb.NewKVServiceClient(f.conn)

	for _, tc := range []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"unicode", "hello世界"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, err := client.Put(
				kvWireContext(t),
				&shardraftpb.PutRequest{
					Header: kvWireHeader(tc.name),
					Key:    tc.name,
					Value:  tc.value,
				},
			)

			kvWireRequireMutation(t, response, err, true)

			kvWireRequireValue(
				t, client, tc.name, tc.value,
			)
		})
	}
}

func TestKVGRPCWireDeduplication(t *testing.T) {
	f := newAdminWireFixture(t)
	electAdminWireLeader(t, f.node)

	client := shardraftpb.NewKVServiceClient(f.conn)
	ctx := kvWireContext(t)

	before := requireAdminWireStatus(t, f)

	request := &shardraftpb.PutRequest{
		Header: kvWireHeader("duplicate-1"),
		Key:    "dedup-key",
		Value:  "original",
	}

	first, err := client.Put(ctx, request)
	kvWireRequireMutation(t, first, err, true)

	committed := requireAdminWireStatus(t, f)

	if committed.CommitIndex != before.CommitIndex+1 {
		t.Fatal("first request did not commit exactly once")
	}

	for i := 0; i < 20; i++ {
		retry, err := client.Put(ctx, request)

		kvWireRequireMutation(t, retry, err, true)

		if retry.Value != first.Value {
			t.Fatal("retry returned a different result")
		}
	}

	after := requireAdminWireStatus(t, f)

	if after.CommitIndex != committed.CommitIndex ||
		after.AppliedIndex != committed.AppliedIndex {
		t.Fatal("duplicate requests advanced the log")
	}

	conflict, err := client.Put(
		ctx,
		&shardraftpb.PutRequest{
			Header: kvWireHeader("duplicate-1"),
			Key:    "dedup-key",
			Value:  "conflicting-value",
		},
	)

	if err != nil ||
		conflict == nil ||
		conflict.Error == nil ||
		conflict.Error.Code !=
			shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT {
		t.Fatalf(
			"unexpected conflict response=%v err=%v",
			conflict, err,
		)
	}

	final := requireAdminWireStatus(t, f)

	if final.CommitIndex != committed.CommitIndex ||
		final.AppliedIndex != committed.AppliedIndex {
		t.Fatal("conflicting request advanced the log")
	}

	kvWireRequireValue(t, client, "dedup-key", "original")

	t.Log("20 identical retries produced no additional commits")
}

func TestKVGRPCWireRoutingValidation(t *testing.T) {
	f := newAdminWireFixture(t)
	electAdminWireLeader(t, f.node)

	client := shardraftpb.NewKVServiceClient(f.conn)

	tests := []struct {
		name   string
		header *shardraftpb.RequestHeader
		want   shardraftpb.ErrorCode
	}{
		{
			name: "MissingHeader",
			want: shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
		},
		{
			name: "WrongShard",
			header: &shardraftpb.RequestHeader{
				ShardId: 999,
				Epoch:   1,
			},
			want: shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
		},
		{
			name: "WrongEpoch",
			header: &shardraftpb.RequestHeader{
				ShardId: defaultShardID,
				Epoch:   999,
			},
			want: shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			response, err := client.Get(
				kvWireContext(t),
				&shardraftpb.GetRequest{
					Header: tc.header,
					Key:    "wire-key",
				},
			)

			if err != nil ||
				response == nil ||
				response.Error == nil ||
				response.Error.Code != tc.want {
				t.Fatalf(
					"response=%v err=%v want=%v",
					response, err, tc.want,
				)
			}
		})
	}
}

func TestKVGRPCWireFollowerMutation(t *testing.T) {
	f := newAdminWireFixture(t)

	state, err := f.node.Status()

	if err != nil || state.Role != raft.Follower {
		t.Fatalf("expected follower: %v err=%v", state, err)
	}

	client := shardraftpb.NewKVServiceClient(f.conn)

	response, err := client.Put(
		kvWireContext(t),
		&shardraftpb.PutRequest{
			Header: kvWireHeader("follower-put"),
			Key:    "wire-key",
			Value:  "must-not-apply",
		},
	)

	if err != nil ||
		response == nil ||
		response.Error == nil ||
		response.Error.Code !=
			shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER {
		t.Fatalf(
			"unexpected follower response=%v err=%v",
			response, err,
		)
	}
}

func TestKVGRPCWireCanceledContext(t *testing.T) {
	f := newAdminWireFixture(t)
	electAdminWireLeader(t, f.node)

	client := shardraftpb.NewKVServiceClient(f.conn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Put(
		ctx,
		&shardraftpb.PutRequest{
			Header: kvWireHeader("canceled-put"),
			Key:    "wire-key",
			Value:  "value",
		},
	)

	if status.Code(err) != codes.Canceled {
		t.Fatalf("error=%v, want Canceled", err)
	}
}

func TestKVGRPCWireExpiredDeadline(t *testing.T) {
	f := newAdminWireFixture(t)
	electAdminWireLeader(t, f.node)

	client := shardraftpb.NewKVServiceClient(f.conn)

	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()

	_, err := client.Get(
		ctx,
		&shardraftpb.GetRequest{
			Header: kvWireHeader("expired-get"),
			Key:    "wire-key",
		},
	)

	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("error=%v, want DeadlineExceeded", err)
	}
}

func TestKVGRPCWireServiceCoexistence(t *testing.T) {
	f := newAdminWireFixture(t)
	electAdminWireLeader(t, f.node)

	client := shardraftpb.NewKVServiceClient(f.conn)
	before := requireAdminWireStatus(t, f)

	response, err := client.Put(
		kvWireContext(t),
		&shardraftpb.PutRequest{
			Header: kvWireHeader("coexistence-put"),
			Key:    "shared-key",
			Value:  "shared-value",
		},
	)

	kvWireRequireMutation(t, response, err, true)

	after := requireAdminWireStatus(t, f)

	if after.CommitIndex != before.CommitIndex+1 ||
		after.AppliedIndex != after.CommitIndex {
		t.Fatalf("unexpected admin status: %v", after)
	}

	health, err := f.client.Health(
		kvWireContext(t),
		&shardraftpb.HealthRequest{},
	)

	if err != nil || health == nil || !health.Healthy {
		t.Fatalf("health=%v err=%v", health, err)
	}

	raftClient := shardraftpb.NewRaftServiceClient(f.conn)

	_, err = raftClient.Step(
		kvWireContext(t),
		&shardraftpb.StepRequest{},
	)

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected Raft validation error: %v", err)
	}

	kvWireRequireValue(
		t, client, "shared-key", "shared-value",
	)

	t.Log("KV, Admin and Raft RPCs coexist successfully")
}
