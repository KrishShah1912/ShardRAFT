package history

import (
	"fmt"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
)

type Verdict string

const (
	Linearizable Verdict = "LINEARIZABLE"
	Illegal      Verdict = "ILLEGAL"
	Inconclusive Verdict = "UNKNOWN"
)

type CheckOptions struct {
	AssumeEmpty bool
	Timeout     time.Duration
}

type KeyResult struct {
	Key        string  `json:"key"`
	Operations int     `json:"operations"`
	Verdict    Verdict `json:"verdict"`
	Reason     string  `json:"reason,omitempty"`
}

type CheckReport struct {
	Verdict     Verdict     `json:"verdict"`
	Operations  int         `json:"operations"`
	Successful  int         `json:"successful"`
	Failed      int         `json:"failed"`
	Unknown     int         `json:"unknown"`
	Pending     int         `json:"pending"`
	AssumeEmpty bool        `json:"assume_empty"`
	DurationNS  int64       `json:"duration_ns"`
	Reason      string      `json:"reason,omitempty"`
	Keys        []KeyResult `json:"keys"`
}

func Check(
	h History,
	options CheckOptions,
) (CheckReport, error) {
	start := time.Now()

	if err := h.Validate(); err != nil {
		return CheckReport{}, err
	}

	if options.Timeout <= 0 {
		return CheckReport{}, fmt.Errorf(
			"checker timeout must be positive",
		)
	}

	report := CheckReport{
		Verdict:     Linearizable,
		Operations:  len(h.Operations),
		AssumeEmpty: options.AssumeEmpty,
		Keys:        make([]KeyResult, 0),
	}

	partitions := make(map[string][]Operation)

	identityKeys := make(map[requestIdentity]string)
	crossKeyIdentity := false

	for _, op := range h.Operations {
		partitions[op.Input.Key] = append(
			partitions[op.Input.Key],
			op,
		)

		switch op.Output.Outcome {
		case Success:
			report.Successful++

		case Failed:
			report.Failed++

		case Unknown:
			report.Unknown++

		case Pending:
			report.Pending++
		}

		if op.Input.Kind != Get {
			identity := requestIdentity{
				ClientID:  op.Input.ClientID,
				RequestID: op.Input.RequestID,
			}

			if key, exists := identityKeys[identity]; exists &&
				key != op.Input.Key {

				crossKeyIdentity = true
			}

			identityKeys[identity] = op.Input.Key
		}
	}

	finish := func() CheckReport {
		report.DurationNS = time.Since(start).Nanoseconds()
		return report
	}

	if len(h.Operations) == 0 {
		report.Verdict = Inconclusive
		report.Reason = "empty history"

		return finish(), nil
	}

	if !options.AssumeEmpty {
		report.Verdict = Inconclusive
		report.Reason = "initial key and dedup state not established"

		return finish(), nil
	}

	if crossKeyIdentity {
		report.Verdict = Inconclusive
		report.Reason = "mutation identity reused across keys"

		return finish(), nil
	}

	keys := make([]string, 0, len(partitions))

	for key := range partitions {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	deadline := start.Add(options.Timeout)

	for _, key := range keys {
		operations := partitions[key]

		result := KeyResult{
			Key:        key,
			Operations: len(operations),
			Verdict:    Linearizable,
		}

		converted := make(
			[]porcupine.Operation,
			0,
			len(operations),
		)

		clientIDs := make(map[string]int)

		for _, op := range operations {
			if op.Output.Outcome != Success {
				result.Verdict = Inconclusive
				result.Reason = "contains unsuccessful or incomplete operations"
				break
			}

			id, exists := clientIDs[op.Input.ClientID]

			if !exists {
				id = len(clientIDs)
				clientIDs[op.Input.ClientID] = id
			}

			converted = append(
				converted,
				porcupine.Operation{
					ClientId: id,
					Input:    op.Input,
					Call:     op.CallNS,
					Output:   op.Output,
					Return:   op.ReturnNS,
				},
			)
		}

		if result.Verdict != Inconclusive {
			remaining := time.Until(deadline)

			if remaining <= 0 {
				result.Verdict = Inconclusive
				result.Reason = "checker timeout"

			} else {
				verdict := porcupine.CheckOperationsTimeout(
					singleKeyModel(),
					converted,
					remaining,
				)

				switch verdict {
				case porcupine.Ok:
					result.Verdict = Linearizable

				case porcupine.Illegal:
					result.Verdict = Illegal
					result.Reason = "no valid sequential execution"

				default:
					result.Verdict = Inconclusive
					result.Reason = "checker timeout or unknown result"
				}
			}
		}

		report.Keys = append(report.Keys, result)

		switch result.Verdict {
		case Illegal:
			report.Verdict = Illegal

		case Inconclusive:
			if report.Verdict != Illegal {
				report.Verdict = Inconclusive
			}
		}
	}

	return finish(), nil
}
