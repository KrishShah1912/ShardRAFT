package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/KrishShah1912/shardraft/internal/client"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type gatewayMutationErrorResponse struct {
	Error     string `json:"error"`
	Code      string `json:"code"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

func gatewayWriteMutationError(
	w http.ResponseWriter,
	err error,
	identity client.MutationIdentity,
) {
	write := func(httpStatus int, code, message string) {
		gatewayWriteJSON(
			w,
			httpStatus,
			gatewayMutationErrorResponse{
				Error:     message,
				Code:      code,
				ClientID:  identity.ClientID,
				RequestID: identity.RequestID,
			},
		)
	}

	if errors.Is(err, client.ErrMutationOutcomeUnknown) {
		write(
			http.StatusGatewayTimeout,
			"outcome_unknown",
			"Commit could not be confirmed. Retry using the original request identity.",
		)
		return
	}

	var failure *client.RoutingFailure

	if errors.As(err, &failure) && failure != nil {
		switch failure.Detail.GetCode() {
		case shardraftpb.ErrorCode_ERROR_CODE_REQUEST_CONFLICT:
			write(
				http.StatusConflict,
				"request_conflict",
				"request identity conflicts with an existing mutation",
			)
			return

		case shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST:
			write(
				http.StatusBadRequest,
				"invalid_request",
				"invalid mutation request",
			)
			return

		case shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE:
			write(
				http.StatusServiceUnavailable,
				"leader_unavailable",
				"cluster unavailable",
			)
			return

		case shardraftpb.ErrorCode_ERROR_CODE_OUTCOME_UNKNOWN:
			write(
				http.StatusGatewayTimeout,
				"outcome_unknown",
				"Retry using the original request identity.",
			)
			return
		}
	}

	switch {
	case errors.Is(err, client.ErrInvalidKey),
		errors.Is(err, client.ErrInvalidValue),
		errors.Is(err, client.ErrInvalidMutationIdentity),
		status.Code(err) == codes.InvalidArgument:
		write(
			http.StatusBadRequest,
			"invalid_request",
			"invalid mutation request",
		)

	case errors.Is(err, context.DeadlineExceeded),
		status.Code(err) == codes.DeadlineExceeded:
		write(
			http.StatusGatewayTimeout,
			"deadline_exceeded",
			"mutation deadline exceeded",
		)

	case errors.Is(err, context.Canceled),
		status.Code(err) == codes.Canceled:
		write(
			http.StatusRequestTimeout,
			"request_canceled",
			"request canceled",
		)

	case errors.Is(err, client.ErrRetryExhausted),
		errors.Is(err, client.ErrMetadataUnavailable),
		errors.Is(err, client.ErrMetadataRefresh),
		status.Code(err) == codes.Unavailable:
		write(
			http.StatusServiceUnavailable,
			"leader_unavailable",
			"cluster unavailable",
		)

	default:
		write(
			http.StatusInternalServerError,
			"internal_error",
			"mutation failed",
		)
	}
}
