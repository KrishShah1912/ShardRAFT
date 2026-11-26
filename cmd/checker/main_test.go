package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KrishShah1912/shardraft/internal/history"
)

func TestCheckerCLI(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value       string
		assumeEmpty bool
		exitCode    int
		verdict     string
	}{
		{
			name:        "linearizable",
			value:       "correct",
			assumeEmpty: true,
			exitCode:    0,
			verdict:     "LINEARIZABLE",
		},
		{
			name:        "illegal",
			value:       "incorrect",
			assumeEmpty: true,
			exitCode:    1,
			verdict:     "ILLEGAL",
		},
		{
			name:     "unknown",
			value:    "correct",
			exitCode: 2,
			verdict:  "UNKNOWN",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := history.NewRecorder()

			id, err := r.Begin(history.Input{
				ClientID:  "client",
				RequestID: "put-1",
				Kind:      history.Put,
				Key:       "key",
				Value:     "correct",
			})

			if err != nil {
				t.Fatal(err)
			}

			if err := r.Complete(id, history.Output{
				Outcome: history.Success,
				Applied: true,
				Value:   "correct",
			}); err != nil {
				t.Fatal(err)
			}

			id, err = r.Begin(history.Input{
				ClientID: "client",
				Kind:     history.Get,
				Key:      "key",
			})

			if err != nil {
				t.Fatal(err)
			}

			if err := r.Complete(id, history.Output{
				Outcome: history.Success,
				Found:   true,
				Value:   tc.value,
			}); err != nil {
				t.Fatal(err)
			}

			path := filepath.Join(
				t.TempDir(),
				"history.json",
			)

			if err := r.Save(path); err != nil {
				t.Fatal(err)
			}

			args := []string{
				"-history", path,
				"-json",
			}

			if tc.assumeEmpty {
				args = append(args, "-assume-empty")
			}

			var stdout, stderr bytes.Buffer

			code := run(args, &stdout, &stderr)

			if code != tc.exitCode {
				t.Fatalf(
					"exit=%d expected=%d stderr=%s",
					code,
					tc.exitCode,
					stderr.String(),
				)
			}

			if !strings.Contains(
				stdout.String(),
				`"verdict": "`+tc.verdict+`"`,
			) {
				t.Fatalf(
					"unexpected output: %s",
					stdout.String(),
				)
			}
		})
	}
}

func TestCheckerCLIInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"-history", "missing.json"},
		{"-history", "missing.json", "-timeout", "0s"},
		{"-invalid-flag"},
	} {
		var stdout, stderr bytes.Buffer

		if code := run(args, &stdout, &stderr); code != 3 {
			t.Fatalf(
				"expected exit 3, got %d for %v",
				code,
				args,
			)
		}
	}
}
