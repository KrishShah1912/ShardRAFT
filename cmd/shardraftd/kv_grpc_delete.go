package main

import (
	"context"

	"github.com/KrishShah1912/shardraft/internal/kvstore"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"

	"google.golang.org/grpc/status"
)

func (s *kvGRPCServer) Delete(
	ctx context.Context,
	request *shardraftpb.DeleteRequest,
) (*shardraftpb.MutationResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if request == nil {
		return &shardraftpb.MutationResponse{
			Error: s.invalidRequest("delete request is required"),
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
		Op:        kvstore.OpDelete,
		Key:       request.Key,
		ClientID:  request.Header.ClientId,
		RequestID: request.Header.RequestId,
	}

	return s.executeMutation(ctx, command)
}
