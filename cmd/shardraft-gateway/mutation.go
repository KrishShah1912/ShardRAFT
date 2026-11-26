package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/KrishShah1912/shardraft/internal/client"
)

const gatewayMaxRequestSize = 1 << 20

type gatewayMutationRequest struct {
	Value     *string `json:"value"`
	Expected  *string `json:"expected"`
	ClientID  string  `json:"client_id"`
	RequestID string  `json:"request_id"`
}

type gatewayMutationResponse struct {
	Applied   bool   `json:"applied"`
	Value     string `json:"value"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

func gatewayDecodeMutation(
	w http.ResponseWriter,
	r *http.Request,
) (gatewayMutationRequest, error) {
	var input gatewayMutationRequest

	r.Body = http.MaxBytesReader(
		w, r.Body, gatewayMaxRequestSize,
	)
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return input, err
	}

	if !utf8.Valid(body) {
		return input, errors.New("invalid UTF-8 body")
	}

	decoder := json.NewDecoder(bytes.NewReader(body))

	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return input, errors.New("expected JSON object")
	}

	seen := make(map[string]bool)

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return input, err
		}

		field, ok := token.(string)
		if !ok || seen[field] {
			return input, errors.New("invalid or duplicate field")
		}

		seen[field] = true

		var value *string

		if err := decoder.Decode(&value); err != nil {
			return input, err
		}

		if value == nil {
			return input, errors.New("null fields are not accepted")
		}

		switch field {
		case "value":
			input.Value = value

		case "expected":
			input.Expected = value

		case "client_id":
			input.ClientID = *value

		case "request_id":
			input.RequestID = *value

		default:
			return input, errors.New("unknown field")
		}
	}

	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return input, errors.New("invalid JSON object")
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		return input, errors.New("expected exactly one JSON object")
	}

	if input.ClientID == "" || input.RequestID == "" {
		return input, errors.New(
			"valid client_id and request_id are required",
		)
	}

	return input, nil
}

func (input gatewayMutationRequest) identity() client.MutationIdentity {
	return client.MutationIdentity{
		ClientID:  input.ClientID,
		RequestID: input.RequestID,
	}
}

func gatewayWriteMutationResult(
	w http.ResponseWriter,
	result client.MutationResult,
	identity client.MutationIdentity,
) {
	gatewayWriteJSON(
		w,
		http.StatusOK,
		gatewayMutationResponse{
			Applied:   result.Applied,
			Value:     result.Value,
			ClientID:  identity.ClientID,
			RequestID: identity.RequestID,
		},
	)
}
