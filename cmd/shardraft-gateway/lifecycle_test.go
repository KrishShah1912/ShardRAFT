package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestGatewayLifecycleSuccessfulStartup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address := listener.Addr().String()

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := defaultGatewayConfig()
	cfg.ListenAddress = address
	cfg.RequestTimeout = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, cfg)
	}()

	httpClient := &http.Client{
		Timeout: time.Second,
	}
	defer httpClient.CloseIdleConnections()

	deadline := time.Now().Add(5 * time.Second)
	ready := false

	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("gateway stopped during startup: %v", err)
		default:
		}

		response, err := httpClient.Get(
			"http://" + address + "/nonexistent",
		)

		if err == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()

			if response.StatusCode != http.StatusNotFound {
				t.Fatalf(
					"unexpected HTTP status: %d",
					response.StatusCode,
				)
			}

			ready = true
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if !ready {
		t.Fatal("gateway did not become ready")
	}

	const workers = 32
	const requestsPerWorker = 10

	var wg sync.WaitGroup
	failures := make(chan error, workers)

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < requestsPerWorker; i++ {
				response, err := httpClient.Get(
					"http://" + address + "/nonexistent",
				)

				if err != nil {
					failures <- err
					return
				}

				_, err = io.Copy(
					io.Discard,
					response.Body,
				)
				response.Body.Close()

				if err != nil {
					failures <- err
					return
				}

				if response.StatusCode != http.StatusNotFound {
					failures <- errors.New(
						"unexpected HTTP response status",
					)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(failures)

	for err := range failures {
		t.Error(err)
	}

	t.Logf(
		"completed %d concurrent HTTP requests",
		workers*requestsPerWorker,
	)

	httpClient.CloseIdleConnections()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gateway did not shut down")
	}

	rebound, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("gateway port was not released: %v", err)
	}
	rebound.Close()
}

func TestGatewayLifecycleUnexpectedServerClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	server := &http.Server{}

	if err := server.Close(); err != nil {
		t.Fatal(err)
	}

	err = serveGateway(
		context.Background(),
		server,
		listener,
		time.Second,
	)

	if err == nil ||
		err.Error() != "gateway stopped unexpectedly" {
		t.Fatalf("unexpected server error: %v", err)
	}
}
