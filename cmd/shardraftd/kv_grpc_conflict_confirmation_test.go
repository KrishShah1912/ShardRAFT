package main

import (
	"errors"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
)

func TestKVGRPCCommittedConflictConfirmation(t *testing.T) {
	tests := []struct {
		name      string
		found     bool
		submitted bool
	}{
		{
			name:  "CommittedBeforeProposal",
			found: true,
		},
		{
			name:      "CommittedAfterProposal",
			found:     true,
			submitted: true,
		},
		{
			name: "UnconfirmedBeforeProposal",
		},
		{
			name:      "UnconfirmedAfterProposal",
			submitted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newKVGRPCServer(1, nil, nil)

			response := server.checkedMutationError(
				kvstore.ErrRequestConflict,
				tc.found,
				tc.submitted,
			)

			detail := response.GetError()

			if detail.GetCode() !=
				shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT {
				t.Fatalf("Unexpected error: %+v", detail)
			}

			if detail.GetConflictCommitted() != tc.found {
				t.Fatalf(
					"ConflictCommitted=%v want=%v",
					detail.GetConflictCommitted(),
					tc.found,
				)
			}

			t.Logf(
				"found=%v submitted=%v confirmed=%v",
				tc.found,
				tc.submitted,
				detail.GetConflictCommitted(),
			)
		})
	}
}

func TestKVGRPCPendingConflictNotConfirmed(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	response := server.mutationError(
		kvstore.ErrRequestConflict,
		false,
	)

	detail := response.GetError()

	if detail.GetCode() !=
		shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT {
		t.Fatalf("Unexpected error: %+v", detail)
	}

	if detail.GetConflictCommitted() {
		t.Fatal("Pending conflict incorrectly marked committed")
	}
}

func TestKVGRPCNonConflictNotConfirmed(t *testing.T) {
	server := newKVGRPCServer(1, nil, nil)

	for _, submitted := range []bool{false, true} {
		response := server.checkedMutationError(
			errors.New("unexpected storage error"),
			true,
			submitted,
		)

		if response.GetError().GetConflictCommitted() {
			t.Fatal("Non-conflict error marked committed")
		}
	}
}
