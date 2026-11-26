package history

import (
	"fmt"
	"testing"
	"time"
)

func testOperation(
	id uint64,
	kind Kind,
	key string,
	value string,
	expected string,
	output Output,
) Operation {
	return Operation{
		ID:       id,
		CallNS:   int64(id * 10),
		ReturnNS: int64(id*10 + 5),
		Input: Input{
			ClientID:  "client",
			RequestID: fmt.Sprintf("request-%d", id),
			Kind:      kind,
			Key:       key,
			Value:     value,
			Expected:  expected,
		},
		Output: output,
	}
}

func checkTestHistory(
	t *testing.T,
	expected Verdict,
	operations ...Operation,
) CheckReport {
	t.Helper()

	report, err := Check(
		History{
			Version:    FormatVersion,
			Operations: operations,
		},
		CheckOptions{
			AssumeEmpty: true,
			Timeout:     5 * time.Second,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if report.Verdict != expected {
		t.Fatalf(
			"expected %s, got %+v",
			expected,
			report,
		)
	}

	return report
}

func TestCheckerLinearizableHistory(t *testing.T) {
	checkTestHistory(
		t,
		Linearizable,
		testOperation(1, Put, "k", "a", "", Output{
			Outcome: Success,
			Applied: true,
			Value:   "a",
		}),
		testOperation(2, Get, "k", "", "", Output{
			Outcome: Success,
			Found:   true,
			Value:   "a",
		}),
	)
}

func TestCheckerRejectsStaleRead(t *testing.T) {
	checkTestHistory(
		t,
		Illegal,
		testOperation(1, Put, "k", "new", "", Output{
			Outcome: Success,
			Applied: true,
			Value:   "new",
		}),
		testOperation(2, Get, "k", "", "", Output{
			Outcome: Success,
			Found:   true,
			Value:   "old",
		}),
	)
}

func TestCheckerOverlappingOperations(t *testing.T) {
	put := testOperation(1, Put, "k", "a", "", Output{
		Outcome: Success,
		Applied: true,
		Value:   "a",
	})

	get := testOperation(2, Get, "k", "", "", Output{
		Outcome: Success,
		Found:   false,
	})

	put.ReturnNS = 30
	get.Input.ClientID = "reader"

	checkTestHistory(t, Linearizable, put, get)
}

func TestCheckerCASAndDelete(t *testing.T) {
	checkTestHistory(
		t,
		Linearizable,
		testOperation(1, Put, "k", "a", "", Output{
			Outcome: Success,
			Applied: true,
			Value:   "a",
		}),
		testOperation(2, CAS, "k", "b", "a", Output{
			Outcome: Success,
			Applied: true,
			Value:   "b",
		}),
		testOperation(3, CAS, "k", "c", "wrong", Output{
			Outcome: Success,
		}),
		testOperation(4, Get, "k", "", "", Output{
			Outcome: Success,
			Found:   true,
			Value:   "b",
		}),
		testOperation(5, Delete, "k", "", "", Output{
			Outcome: Success,
			Applied: true,
		}),
		testOperation(6, Get, "k", "", "", Output{
			Outcome: Success,
		}),
		testOperation(7, Delete, "k", "", "", Output{
			Outcome: Success,
		}),
	)
}

func TestCheckerRejectsIncorrectCAS(t *testing.T) {
	checkTestHistory(
		t,
		Illegal,
		testOperation(1, CAS, "missing", "new", "", Output{
			Outcome: Success,
			Applied: true,
			Value:   "new",
		}),
	)
}

func TestCheckerDuplicateMutation(t *testing.T) {
	first := testOperation(1, Put, "k", "a", "", Output{
		Outcome: Success,
		Applied: true,
		Value:   "a",
	})

	second := testOperation(2, Put, "k", "b", "", Output{
		Outcome: Success,
		Applied: true,
		Value:   "b",
	})

	retry := first
	retry.ID = 3
	retry.CallNS = 30
	retry.ReturnNS = 35

	get := testOperation(4, Get, "k", "", "", Output{
		Outcome: Success,
		Found:   true,
		Value:   "b",
	})

	checkTestHistory(
		t,
		Linearizable,
		first,
		second,
		retry,
		get,
	)

	get.Output.Value = "a"

	checkTestHistory(
		t,
		Illegal,
		first,
		second,
		retry,
		get,
	)
}

func TestCheckerRejectsConflictingSuccessfulRetry(t *testing.T) {
	first := testOperation(1, Put, "k", "a", "", Output{
		Outcome: Success,
		Applied: true,
		Value:   "a",
	})

	retry := first
	retry.ID = 2
	retry.CallNS = 20
	retry.ReturnNS = 25
	retry.Input.Value = "b"
	retry.Output.Value = "b"

	checkTestHistory(t, Illegal, first, retry)
}

func TestCheckerKeyPartitioning(t *testing.T) {
	report := checkTestHistory(
		t,
		Linearizable,
		testOperation(1, Put, "a", "value-a", "", Output{
			Outcome: Success,
			Applied: true,
			Value:   "value-a",
		}),
		testOperation(2, Put, "b", "value-b", "", Output{
			Outcome: Success,
			Applied: true,
			Value:   "value-b",
		}),
		testOperation(3, Get, "a", "", "", Output{
			Outcome: Success,
			Found:   true,
			Value:   "value-a",
		}),
		testOperation(4, Get, "b", "", "", Output{
			Outcome: Success,
			Found:   true,
			Value:   "value-b",
		}),
	)

	if len(report.Keys) != 2 {
		t.Fatalf("expected 2 key partitions")
	}
}

func TestCheckerUncertainOperations(t *testing.T) {
	for _, outcome := range []Outcome{
		Failed,
		Unknown,
		Pending,
	} {
		t.Run(string(outcome), func(t *testing.T) {
			op := testOperation(
				1, Put, "k", "a", "",
				Output{
					Outcome: outcome,
					Error:   "request unsuccessful",
				},
			)

			if outcome == Pending {
				op.ReturnNS = 0
				op.Output.Error = ""
			}

			checkTestHistory(t, Inconclusive, op)
		})
	}
}

func TestCheckerUnknownInitialState(t *testing.T) {
	report, err := Check(
		History{
			Version: FormatVersion,
			Operations: []Operation{
				testOperation(1, Get, "k", "", "", Output{
					Outcome: Success,
				}),
			},
		},
		CheckOptions{
			Timeout: time.Second,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if report.Verdict != Inconclusive {
		t.Fatalf("unexpected verdict: %s", report.Verdict)
	}
}

func TestCheckerEmptyHistory(t *testing.T) {
	checkTestHistory(t, Inconclusive)
}

func TestCheckerCrossKeyIdentity(t *testing.T) {
	first := testOperation(1, Put, "a", "a", "", Output{
		Outcome: Success,
		Applied: true,
		Value:   "a",
	})

	second := testOperation(2, Put, "b", "b", "", Output{
		Outcome: Success,
		Applied: true,
		Value:   "b",
	})

	second.Input.RequestID = first.Input.RequestID

	checkTestHistory(t, Inconclusive, first, second)
}

func TestCheckerEmptyValue(t *testing.T) {
	checkTestHistory(
		t,
		Linearizable,
		testOperation(1, Put, "k", "", "", Output{
			Outcome: Success,
			Applied: true,
		}),
		testOperation(2, Get, "k", "", "", Output{
			Outcome: Success,
			Found:   true,
		}),
	)
}

func TestCheckerIllegalPartitionTakesPrecedence(t *testing.T) {
	checkTestHistory(
		t,
		Illegal,
		testOperation(1, Put, "a", "a", "", Output{
			Outcome: Unknown,
			Error:   "timeout",
		}),
		testOperation(2, Get, "b", "", "", Output{
			Outcome: Success,
			Found:   true,
			Value:   "unexpected",
		}),
	)
}
