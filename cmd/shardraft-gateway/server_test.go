package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGatewayServerDefaultFlags(t *testing.T) {
	cfg, err := parseGatewayFlags(nil, io.Discard)

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(cfg, defaultGatewayConfig()) {
		t.Fatalf("unexpected configuration: %+v", cfg)
	}
}

func TestGatewayServerCustomFlags(t *testing.T) {
	cfg, err := parseGatewayFlags(
		[]string{
			"-http-addr", "127.0.0.1:8185",
			"-grpc-endpoints",
			"127.0.0.1:9191,127.0.0.1:9192",
			"-request-timeout", "4s",
		},
		io.Discard,
	)

	if err != nil {
		t.Fatal(err)
	}

	if cfg.ListenAddress != "127.0.0.1:8185" ||
		cfg.RequestTimeout != 4*time.Second ||
		!reflect.DeepEqual(
			cfg.Endpoints,
			[]string{
				"127.0.0.1:9191",
				"127.0.0.1:9192",
			},
		) {
		t.Fatalf("unexpected configuration: %+v", cfg)
	}
}

func TestGatewayServerInvalidFlags(t *testing.T) {
	cases := [][]string{
		{"-unknown"},
		{"-http-addr", "invalid"},
		{"-grpc-endpoints", ""},
		{"-grpc-endpoints", "localhost:0"},
		{"-request-timeout", "invalid"},
		{"-request-timeout", "0s"},
		{"-request-timeout", "-1s"},
		{"unexpected"},
	}

	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			if _, err := parseGatewayFlags(
				args, io.Discard,
			); err == nil {
				t.Fatal("expected flag validation error")
			}
		})
	}
}

func TestGatewayServerHelp(t *testing.T) {
	var output strings.Builder

	_, err := parseGatewayFlags(
		[]string{"-help"},
		&output,
	)

	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, name := range []string{
		"http-addr",
		"grpc-endpoints",
		"request-timeout",
	} {
		if !strings.Contains(output.String(), name) {
			t.Fatalf("missing help flag: %s", name)
		}
	}
}

func TestGatewayServerHTTPConfiguration(t *testing.T) {
	cfg := defaultGatewayConfig()

	server := newGatewayHTTPServer(
		cfg,
		&gatewayReadMock{},
	)

	if server.Addr != cfg.ListenAddress ||
		server.Handler == nil ||
		server.ReadHeaderTimeout != 5*time.Second ||
		server.ReadTimeout != 10*time.Second ||
		server.WriteTimeout != cfg.RequestTimeout+15*time.Second ||
		server.IdleTimeout != 60*time.Second ||
		server.MaxHeaderBytes != 1<<20 {
		t.Fatalf("unexpected server configuration: %+v", server)
	}
}

func TestGatewayServerGracefulShutdown(t *testing.T) {
	listener, err := net.Listen(
		"tcp", "127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	release := make(chan struct{})

	server := &http.Server{
		Handler: http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				close(started)
				<-release
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("completed"))
			},
		),
	}
	defer server.Close()

	done := make(chan error, 1)

	go func() {
		done <- serveGateway(
			ctx, server, listener, 3*time.Second,
		)
	}()

	requestDone := make(chan error, 1)

	go func() {
		c := &http.Client{Timeout: 5 * time.Second}

		response, err := c.Get(
			"http://" + listener.Addr().String(),
		)
		if err != nil {
			requestDone <- err
			return
		}
		defer response.Body.Close()

		body, err := io.ReadAll(response.Body)
		if err == nil &&
			(response.StatusCode != 200 ||
				string(body) != "completed") {
			err = errors.New("in-flight request did not complete")
		}

		requestDone <- err
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("request did not reach server")
	}

	cancel()

	select {
	case err := <-done:
		close(release)
		t.Fatalf("server stopped before request completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request did not complete")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gateway did not shut down")
	}

	connection, err := net.DialTimeout(
		"tcp",
		listener.Addr().String(),
		200*time.Millisecond,
	)

	if err == nil {
		connection.Close()
		t.Fatal("listener still accepts connections")
	}
}

func TestGatewayServerForcedShutdown(t *testing.T) {
	listener, err := net.Listen(
		"tcp", "127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	handlerDone := make(chan struct{})

	server := &http.Server{
		Handler: http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				defer close(handlerDone)
				close(started)
				<-r.Context().Done()
			},
		),
	}
	defer server.Close()

	done := make(chan error, 1)

	go func() {
		done <- serveGateway(
			ctx, server, listener, 50*time.Millisecond,
		)
	}()

	requestDone := make(chan error, 1)

	go func() {
		c := &http.Client{Timeout: 5 * time.Second}

		response, err := c.Get(
			"http://" + listener.Addr().String(),
		)

		if response != nil {
			response.Body.Close()
		}

		requestDone <- err
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not reach server")
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unexpected shutdown error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("forced shutdown timed out")
	}

	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("handler context was not canceled")
	}

	select {
	case err := <-requestDone:
		if err == nil {
			t.Fatal("expected interrupted HTTP request")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP client did not return")
	}
}

func TestGatewayServerListenerFailure(t *testing.T) {
	listener, err := net.Listen(
		"tcp", "127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	server := &http.Server{}

	err = serveGateway(
		context.Background(),
		server,
		listener,
		time.Second,
	)

	if err == nil {
		t.Fatal("expected listener failure")
	}
}

func TestGatewayServerInvalidConfiguration(t *testing.T) {
	cfg := defaultGatewayConfig()
	cfg.Endpoints = nil

	if err := runGateway(
		context.Background(), cfg,
	); err == nil {
		t.Fatal("expected configuration error")
	}
}

func TestGatewayServerCanceledStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runGateway(ctx, defaultGatewayConfig())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayServerOccupiedAddress(t *testing.T) {
	listener, err := net.Listen(
		"tcp", "127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	cfg := defaultGatewayConfig()
	cfg.ListenAddress = listener.Addr().String()

	err = runGateway(context.Background(), cfg)

	if err == nil ||
		!strings.Contains(err.Error(), "start gateway listener") {
		t.Fatalf("unexpected error: %v", err)
	}
}
