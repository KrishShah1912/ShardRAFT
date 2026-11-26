package main

import (
	"context"
	"net/http"
	"unicode/utf8"
)

func (h *gatewayHandler) handleCAS(
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
		input.Expected == nil ||
		!utf8.ValidString(*input.Value) ||
		!utf8.ValidString(*input.Expected) {
		gatewayWriteError(
			w,
			http.StatusBadRequest,
			"invalid_request",
			"CAS requires expected and value",
		)
		return
	}

	identity := input.identity()

	ctx, cancel := context.WithTimeout(
		r.Context(),
		h.timeout,
	)
	defer cancel()

	result, err := h.client.CompareAndSwapWithIdentity(
		ctx,
		key,
		*input.Expected,
		*input.Value,
		identity,
	)

	if err != nil {
		gatewayWriteMutationError(w, err, identity)
		return
	}

	gatewayWriteMutationResult(w, result, identity)
}
