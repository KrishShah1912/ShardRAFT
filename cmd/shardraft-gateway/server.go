package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

const gatewayShutdownTimeout = 15 * time.Second

func newGatewayHTTPServer(
	cfg gatewayConfig,
	c gatewayKVClient,
) *http.Server {
	return &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           newGatewayHandler(c, cfg.RequestTimeout).routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.RequestTimeout + 15*time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

func serveGateway(
	ctx context.Context,
	server *http.Server,
	listener net.Listener,
	shutdownTimeout time.Duration,
) error {
	serverErrors := make(chan error, 1)

	go func() {
		serverErrors <- server.Serve(listener)
	}()

	select {
	case err := <-serverErrors:
		_ = server.Close()

		if errors.Is(err, http.ErrServerClosed) {
			return errors.New("gateway stopped unexpectedly")
		}

		return fmt.Errorf("gateway server failed: %w", err)

	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			shutdownTimeout,
		)
		defer cancel()

		shutdownErr := server.Shutdown(shutdownCtx)

		if shutdownErr != nil {
			closeErr := server.Close()
			serveErr := <-serverErrors

			if errors.Is(serveErr, http.ErrServerClosed) {
				serveErr = nil
			}

			return errors.Join(
				fmt.Errorf("gateway shutdown: %w", shutdownErr),
				closeErr,
				serveErr,
			)
		}

		err := <-serverErrors

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("gateway server failed: %w", err)
		}

		return nil
	}
}

func runGateway(
	ctx context.Context,
	cfg gatewayConfig,
) (resultErr error) {
	if err := cfg.Validate(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	c, err := newGatewayClient(cfg)
	if err != nil {
		return fmt.Errorf("create gateway client: %w", err)
	}

	defer func() {
		if err := c.Close(); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()

	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("start gateway listener: %w", err)
	}
	defer listener.Close()

	server := newGatewayHTTPServer(cfg, c)

	log.Printf(
		"ShardRAFT HTTP gateway listening on %s",
		listener.Addr(),
	)

	log.Printf(
		"gRPC bootstrap endpoints: %v",
		cfg.Endpoints,
	)

	err = serveGateway(
		ctx,
		server,
		listener,
		gatewayShutdownTimeout,
	)

	if err == nil {
		log.Print("ShardRAFT HTTP gateway stopped")
	}

	return err
}
