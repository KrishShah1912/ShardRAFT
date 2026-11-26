package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"unicode/utf8"

	"github.com/KrishShah1912/shardraft/internal/history"
)

type gatewayResponse struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Found     bool   `json:"found"`
	Applied   bool   `json:"applied"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
	Error     string `json:"error"`
	Code      string `json:"code"`
}

func unsuccessful(in history.Input, err error) history.Output {
	outcome := history.Failed

	if in.Kind != history.Get {
		outcome = history.Unknown
	}

	return history.Output{
		Outcome: outcome,
		Error:   err.Error(),
	}
}

func execute(
	ctx context.Context,
	client *http.Client,
	target string,
	in history.Input,
) history.Output {
	endpoint := target + "/kv/" + url.PathEscape(in.Key)

	method := http.MethodGet
	var body io.Reader

	switch in.Kind {
	case history.Get:
	case history.Put:
		method = http.MethodPut

		data, err := json.Marshal(map[string]string{
			"value":      in.Value,
			"client_id":  in.ClientID,
			"request_id": in.RequestID,
		})

		if err != nil {
			return unsuccessful(in, err)
		}

		body = bytes.NewReader(data)

	default:
		return unsuccessful(in, fmt.Errorf(
			"unsupported operation: %s", in.Kind,
		))
	}

	req, err := http.NewRequestWithContext(
		ctx, method, endpoint, body,
	)

	if err != nil {
		return unsuccessful(in, err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)

	if err != nil {
		return unsuccessful(in, err)
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(
		io.LimitReader(resp.Body, 2*1024*1024+1),
	)

	if err != nil {
		return unsuccessful(in, err)
	}

	if len(data) > 2*1024*1024 || !utf8.Valid(data) {
		return unsuccessful(in, fmt.Errorf("invalid response body"))
	}

	var result gatewayResponse

	if err := json.Unmarshal(data, &result); err != nil {
		return unsuccessful(in, fmt.Errorf(
			"HTTP %d: invalid JSON response: %w",
			resp.StatusCode, err,
		))
	}

	// Missing-key GET responses contain an error code,
	// not the normal key/value response.
	if in.Kind == history.Get &&
		resp.StatusCode == http.StatusNotFound {

		if result.Code != "key_not_found" {
			return unsuccessful(in, fmt.Errorf(
				"unexpected HTTP 404 response: %s",
				result.Code,
			))
		}

		return history.Output{
			Outcome: history.Success,
			Found:   false,
		}
	}

	if resp.StatusCode != http.StatusOK {
		return unsuccessful(in, fmt.Errorf(
			"gateway returned HTTP %d: %s %s",
			resp.StatusCode,
			result.Code,
			result.Error,
		))
	}

	if result.Error != "" || result.Code != "" {
		return unsuccessful(in, fmt.Errorf(
			"unexpected error in successful response: %s %s",
			result.Code, result.Error,
		))
	}

	if in.Kind == history.Get {
		if result.Key != in.Key {
			return unsuccessful(in, fmt.Errorf(
				"response key mismatch",
			))
		}

		if !result.Found {
			return unsuccessful(in, fmt.Errorf(
				"inconsistent GET response",
			))
		}

		return history.Output{
			Outcome: history.Success,
			Found:   true,
			Value:   result.Value,
		}
	}

	// Mutation responses identify requests using
	// client_id and request_id, not the key.
	if result.ClientID != in.ClientID ||
		result.RequestID != in.RequestID {
		return unsuccessful(in, fmt.Errorf(
			"mutation identity mismatch",
		))
	}

	if !result.Applied {
		return unsuccessful(in, fmt.Errorf(
			"PUT was not applied",
		))
	}

	return history.Output{
		Outcome: history.Success,
		Applied: result.Applied,
		Value:   result.Value,
	}
}
