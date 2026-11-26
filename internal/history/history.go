package history

import (
	"fmt"
	"unicode/utf8"
)

type Kind string

const (
	Get    Kind = "get"
	Put    Kind = "put"
	Delete Kind = "delete"
	CAS    Kind = "cas"
)

type Outcome string

const (
	Pending Outcome = "pending"
	Success Outcome = "success"
	Failed  Outcome = "failed"
	Unknown Outcome = "unknown"
)

type Input struct {
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id,omitempty"`
	Kind      Kind   `json:"kind"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Expected  string `json:"expected"`
}

type Output struct {
	Value   string  `json:"value"`
	Found   bool    `json:"found"`
	Applied bool    `json:"applied"`
	Outcome Outcome `json:"outcome"`
	Error   string  `json:"error,omitempty"`
}

type Operation struct {
	ID       uint64 `json:"id"`
	CallNS   int64  `json:"call_ns"`
	ReturnNS int64  `json:"return_ns"`
	Input    Input  `json:"input"`
	Output   Output `json:"output"`
}

type History struct {
	Version    int         `json:"version"`
	Operations []Operation `json:"operations"`
}

const FormatVersion = 1

func (in Input) Validate() error {
	if in.ClientID == "" {
		return fmt.Errorf("client ID is required")
	}

	if in.Key == "" {
		return fmt.Errorf("key is required")
	}

	for _, value := range []string{
		in.ClientID,
		in.RequestID,
		in.Key,
		in.Value,
		in.Expected,
	} {
		if !utf8.ValidString(value) {
			return fmt.Errorf("invalid UTF-8 input")
		}
	}

	switch in.Kind {
	case Get:
		return nil

	case Put, Delete, CAS:
		if in.RequestID == "" {
			return fmt.Errorf("mutation request ID is required")
		}
		return nil

	default:
		return fmt.Errorf("unsupported operation: %q", in.Kind)
	}
}

func (out Output) Validate() error {
	if !utf8.ValidString(out.Value) ||
		!utf8.ValidString(out.Error) {
		return fmt.Errorf("invalid UTF-8 output")
	}

	switch out.Outcome {
	case Success:
		if out.Error != "" {
			return fmt.Errorf("successful operation contains error")
		}

	case Failed, Unknown:
		if out.Error == "" {
			return fmt.Errorf("unsuccessful operation requires error")
		}

	default:
		return fmt.Errorf("invalid completed outcome: %q", out.Outcome)
	}

	return nil
}

func (h History) Validate() error {
	if h.Version != FormatVersion {
		return fmt.Errorf("unsupported history version: %d", h.Version)
	}

	seen := make(map[uint64]struct{}, len(h.Operations))

	for _, op := range h.Operations {
		if op.ID == 0 {
			return fmt.Errorf("operation ID must be positive")
		}

		if _, exists := seen[op.ID]; exists {
			return fmt.Errorf("duplicate operation ID: %d", op.ID)
		}

		seen[op.ID] = struct{}{}

		if err := op.Input.Validate(); err != nil {
			return fmt.Errorf("operation %d: %w", op.ID, err)
		}

		if op.CallNS < 0 {
			return fmt.Errorf("operation %d: invalid call time", op.ID)
		}

		if op.Output.Outcome == Pending {
			if op.ReturnNS != 0 {
				return fmt.Errorf(
					"operation %d: pending operation has return time",
					op.ID,
				)
			}

			continue
		}

		if op.ReturnNS <= op.CallNS {
			return fmt.Errorf(
				"operation %d: invalid return time",
				op.ID,
			)
		}

		if err := op.Output.Validate(); err != nil {
			return fmt.Errorf("operation %d: %w", op.ID, err)
		}
	}

	return nil
}
