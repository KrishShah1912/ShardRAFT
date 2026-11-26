package main

import (
	"context"
	"net/http"
	"unicode/utf8"
)

func (h *gatewayHandler) handleDelete(
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

	if input.Value != nil || input.Expected != nil {
		gatewayWriteError(
			w,
			http.StatusBadRequest,
			"invalid_request",
			"DELETE does not accept value or expected",
		)
		return
	}

	identity := input.identity()

	ctx, cancel := context.WithTimeout(
		r.Context(),
		h.timeout,
	)
	defer cancel()

	result, err := h.client.DeleteWithIdentity(
		ctx,
		key,
		identity,
	)

	if err != nil {
		gatewayWriteMutationError(w, err, identity)
		return
	}

	gatewayWriteMutationResult(w, result, identity)
}
