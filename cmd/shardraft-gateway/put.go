package main

import (
	"context"
	"net/http"
	"unicode/utf8"
)

func (h *gatewayHandler) handlePut(
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

	input, err := gatewayDecodeMutation(w, r)

	if err != nil {
		gatewayWriteError(
			w,
			http.StatusBadRequest,
			"invalid_request",
			"invalid mutation body",
		)
		return
	}

	if input.Value == nil ||
		!utf8.ValidString(*input.Value) ||
		input.Expected != nil {
		gatewayWriteError(
			w,
			http.StatusBadRequest,
			"invalid_request",
			"PUT requires a value and does not accept expected",
		)
		return
	}

	identity := input.identity()

	ctx, cancel := context.WithTimeout(
		r.Context(),
		h.timeout,
	)
	defer cancel()

	result, err := h.client.PutWithIdentity(
		ctx,
		key,
		*input.Value,
		identity,
	)

	if err != nil {
		gatewayWriteMutationError(w, err, identity)
		return
	}

	gatewayWriteMutationResult(w, result, identity)
}
