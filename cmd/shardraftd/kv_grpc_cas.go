package main

import (
	"context"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/status"
)

func (s *kvGRPCServer) CompareAndSwap(
	ctx context.Context,
	request *shardraftpb.CompareAndSwapRequest,
) (*shardraftpb.MutationResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if request == nil {
		return &shardraftpb.MutationResponse{
			Error: s.invalidRequest("compare and swap request is required"),
		}, nil
	}

	if failure := s.validateRequestSize(request); failure != nil {
		return &shardraftpb.MutationResponse{
			Error: failure,
		}, nil
	}

	if failure := s.validateHeader(
		request.Header,
		true,
	); failure != nil {
		return &shardraftpb.MutationResponse{
			Error: failure,
		}, nil
	}

	command := kvstore.Command{
		Op:        kvstore.OpCompareAndSwap,
		Key:       request.Key,
		Expected:  request.Expected,
		Value:     request.Value,
		ClientID:  request.Header.ClientId,
		RequestID: request.Header.RequestId,
	}

	return s.executeMutation(ctx, command)
}
