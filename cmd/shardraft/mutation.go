package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/KrishShah1912/shardraft/internal/client"
)

const cliExitOutcomeUnknown = 4

type cliMutationResult struct {
	Operation cliOperation `json:"operation"`
	Key       string       `json:"key"`
	Value     string       `json:"value"`
	Applied   bool         `json:"applied"`
	ClientID  string       `json:"client_id"`
	RequestID string       `json:"request_id"`
}

func validateCLIMutationIdentity(
	identity client.MutationIdentity,
) error {
	if err := identity.Validate(); err != nil {
		return err
	}

	if !utf8.ValidString(identity.ClientID) ||
		!utf8.ValidString(identity.RequestID) {
		return errors.New(
			"mutation identities must contain valid UTF-8",
		)
	}

	return nil
}

func resolveCLIMutationIdentity(
	kv cliKVClient,
	cfg cliConfig,
) (client.MutationIdentity, error) {
	if cfg.RequestID != "" {
		identity := client.MutationIdentity{
			ClientID:  cfg.ClientID,
			RequestID: cfg.RequestID,
		}

		if err := validateCLIMutationIdentity(identity); err != nil {
			return client.MutationIdentity{}, err
		}

		return identity, nil
	}

	identity, err := kv.NewMutationIdentity()

	if err != nil {
		return client.MutationIdentity{}, err
	}

	if err := validateCLIMutationIdentity(identity); err != nil {
		return client.MutationIdentity{}, err
	}

	if cfg.ClientID != "" && identity.ClientID != cfg.ClientID {
		return client.MutationIdentity{},
			errors.New("generated client identity does not match configuration")
	}

	return identity, nil
}

func writeCLIMutationResult(
	output io.Writer,
	format string,
	result cliMutationResult,
) error {
	switch format {
	case "json":
		return json.NewEncoder(output).Encode(result)

	case "text":
		_, err := fmt.Fprintf(
			output,
			"Operation: %s\nKey: %q\nValue: %q\nApplied: %t\nClient ID: %q\nRequest ID: %q\n",
			result.Operation,
			result.Key,
			result.Value,
			result.Applied,
			result.ClientID,
			result.RequestID,
		)

		return err

	default:
		return fmt.Errorf("unsupported output format: %q", format)
	}
}

func writeCLIMutationFailure(
	stderr io.Writer,
	operation cliOperation,
	identity client.MutationIdentity,
	err error,
) int {
	fmt.Fprintf(
		stderr,
		"error: %s failed: %v\n",
		operation,
		err,
	)

	fmt.Fprintf(
		stderr,
		"Client ID: %q\nRequest ID: %q\n",
		identity.ClientID,
		identity.RequestID,
	)

	if errors.Is(err, client.ErrMutationOutcomeUnknown) {
		fmt.Fprintln(
			stderr,
			"Mutation outcome is unknown. Retry the same operation with the original client-id and request-id.",
		)

		return cliExitOutcomeUnknown
	}

	return cliExitFailure
}

func completeCLIMutation(
	invocation cliInvocation,
	identity client.MutationIdentity,
	response client.MutationResult,
	stdout io.Writer,
	stderr io.Writer,
) int {
	result := cliMutationResult{
		Operation: invocation.Command.Operation,
		Key:       invocation.Command.Key,
		Value:     response.Value,
		Applied:   response.Applied,
		ClientID:  identity.ClientID,
		RequestID: identity.RequestID,
	}

	if err := writeCLIMutationResult(
		stdout,
		invocation.Config.Output,
		result,
	); err != nil {
		fmt.Fprintf(
			stderr,
			"error: mutation response received but output failed: %v\n",
			err,
		)

		fmt.Fprintf(
			stderr,
			"Client ID: %q\nRequest ID: %q\n",
			identity.ClientID,
			identity.RequestID,
		)

		fmt.Fprintln(
			stderr,
			"To recover the original result, retry using the same mutation identity.",
		)

		return cliExitFailure
	}

	return cliExitSuccess
}
