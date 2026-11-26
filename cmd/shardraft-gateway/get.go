package main

import (
	"context"
	"errors"
	"net/http"
	"unicode/utf8"

	"github.com/KrishShah1912/shardraft/internal/client"
	shardraftpb "github.com/KrishShah1912/shardraft/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type gatewayGetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Found bool   `json:"found"`
}

func (h *gatewayHandler) handleGet(
	w http.ResponseWriter,
	r *http.Request,
) {
	key := r.PathValue("key")

	if key == "" || !utf8.ValidString(key) {
		gatewayWriteError(
			w,
			http.StatusBadRequest,
			"invalid_request",
			"invalid key",
		)
		return
	}

	mode := r.URL.Query().Get("read_mode")

	if mode != "" &&
		mode != "linearizable" &&
		mode != "local" {
		gatewayWriteError(
			w,
			http.StatusBadRequest,
			"invalid_request",
			"invalid read mode",
		)
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		h.timeout,
	)
	defer cancel()

	var value string
	var found bool
	var err error

	if mode == "local" {
		value, found, err = h.client.GetLocal(ctx, key)
	} else {
		value, found, err = h.client.Get(ctx, key)
	}

	if err != nil {
		gatewayWriteReadError(w, err)
		return
	}

	if !found {
		gatewayWriteError(
			w,
			http.StatusNotFound,
			"key_not_found",
			"key not found",
		)
		return
	}

	gatewayWriteJSON(
		w,
		http.StatusOK,
		gatewayGetResponse{
			Key:   key,
			Value: value,
			Found: true,
		},
	)
}

func gatewayWriteReadError(
	w http.ResponseWriter,
	err error,
) {
	switch {
	case errors.Is(err, context.DeadlineExceeded),
		status.Code(err) == codes.DeadlineExceeded:
		gatewayWriteError(
			w, http.StatusGatewayTimeout,
			"deadline_exceeded", "read deadline exceeded",
		)
		return

	case errors.Is(err, context.Canceled),
		status.Code(err) == codes.Canceled:
		gatewayWriteError(
			w, http.StatusRequestTimeout,
			"request_canceled", "request canceled",
		)
		return
	}

	var failure *client.RoutingFailure

	if errors.As(err, &failure) && failure != nil {
		switch failure.Detail.GetCode() {
		case shardraftpb.ErrorCode_ERROR_CODE_INVALID_REQUEST:
			gatewayWriteError(
				w, http.StatusBadRequest,
				"invalid_request", "invalid read request",
			)
			return

		case shardraftpb.ErrorCode_ERROR_CODE_KEY_NOT_FOUND:
			gatewayWriteError(
				w, http.StatusNotFound,
				"key_not_found", "key not found",
			)
			return

		case shardraftpb.ErrorCode_ERROR_CODE_NOT_LEADER,
			shardraftpb.ErrorCode_ERROR_CODE_WRONG_SHARD,
			shardraftpb.ErrorCode_ERROR_CODE_LEADER_UNAVAILABLE:
			gatewayWriteError(
				w, http.StatusServiceUnavailable,
				"leader_unavailable", "cluster unavailable",
			)
			return
		}
	}

	switch {
	case errors.Is(err, client.ErrInvalidKey),
		status.Code(err) == codes.InvalidArgument:
		gatewayWriteError(
			w, http.StatusBadRequest,
			"invalid_request", "invalid read request",
		)

	case errors.Is(err, client.ErrRetryExhausted),
		errors.Is(err, client.ErrMetadataUnavailable),
		errors.Is(err, client.ErrMetadataRefresh),
		status.Code(err) == codes.Unavailable:
		gatewayWriteError(
			w, http.StatusServiceUnavailable,
			"leader_unavailable", "cluster unavailable",
		)

	default:
		gatewayWriteError(
			w, http.StatusInternalServerError,
			"internal_error", "read failed",
		)
	}
}
