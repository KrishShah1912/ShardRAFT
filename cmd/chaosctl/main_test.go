package main

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestComposeArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "status",
			args: []string{"status"},
			want: []string{"ps", "-a"},
		},
		{
			name: "stop",
			args: []string{"stop", "n1"},
			want: []string{"stop", "n1"},
		},
		{
			name: "start",
			args: []string{"start", "n3"},
			want: []string{"start", "n3"},
		},
		{
			name: "restart",
			args: []string{"restart", "n5"},
			want: []string{"restart", "n5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := composeArgs(tt.args)

			if err != nil {
				t.Fatal(err)
			}

			want := append(
				[]string{
					"compose",
					"-f",
					"docker-compose.yml",
					"-p",
					"shardraft",
				},
				tt.want...,
			)

			if !reflect.DeepEqual(got, want) {
				t.Fatalf(
					"got=%v want=%v",
					got,
					want,
				)
			}
		})
	}
}

func TestComposeArgsInvalid(t *testing.T) {
	tests := [][]string{
		nil,
		{"unknown"},
		{"status", "n1"},
		{"stop"},
		{"start", "n0"},
		{"restart", "n6"},
		{"stop", "gateway1"},
		{"stop", "n1", "n2"},
		{"stop", "--all"},
		{"down", "-v"},
	}

	for _, args := range tests {
		if _, err := composeArgs(args); err == nil {
			t.Errorf(
				"expected rejection for %v",
				args,
			)
		}
	}
}

func TestRunSuccess(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	called := false

	code := run(
		context.Background(),
		[]string{"stop", "n2"},
		&stdout,
		&stderr,
		func(
			_ context.Context,
			args []string,
		) error {
			called = true

			if args[len(args)-1] != "n2" {
				t.Fatalf("unexpected args: %v", args)
			}

			return nil
		},
	)

	if code != 0 || !called {
		t.Fatalf("code=%d called=%t", code, called)
	}
}

func TestRunFailure(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := run(
		context.Background(),
		[]string{"restart", "n1"},
		&stdout,
		&stderr,
		func(
			_ context.Context,
			_ []string,
		) error {
			return errors.New("docker unavailable")
		},
	)

	if code != 1 {
		t.Fatalf("unexpected exit code: %d", code)
	}

	if !bytes.Contains(
		stderr.Bytes(),
		[]byte("docker unavailable"),
	) {
		t.Fatal("missing Docker error")
	}
}

func TestRunInvalidAndHelp(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0},
		{[]string{"help"}, 0},
		{[]string{"stop", "n9"}, 2},
		{nil, 2},
	} {
		var stdout bytes.Buffer
		var stderr bytes.Buffer

		code := run(
			context.Background(),
			tc.args,
			&stdout,
			&stderr,
			func(
				_ context.Context,
				_ []string,
			) error {
				t.Fatal("unexpected Docker execution")
				return nil
			},
		)

		if code != tc.code {
			t.Fatalf(
				"args=%v code=%d want=%d",
				tc.args,
				code,
				tc.code,
			)
		}
	}
}
