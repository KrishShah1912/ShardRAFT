package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/KrishShah1912/shardraft/internal/history"
)

type RunResult struct {
	RunID    string
	Config   Config
	Duration time.Duration
	History  history.History
	Targets  map[uint64]string
}

func runBenchmark(ctx context.Context, cfg Config) (RunResult, error) {
	if err := cfg.Validate(); err != nil {
		return RunResult{}, err
	}

	randomID := make([]byte, 16)

	if _, err := rand.Read(randomID); err != nil {
		return RunResult{}, err
	}

	runID := hex.EncodeToString(randomID)

	recorder := history.NewRecorder()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = cfg.Concurrency * 2
	transport.MaxIdleConnsPerHost = cfg.Concurrency
	transport.MaxConnsPerHost = cfg.Concurrency

	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
		CheckRedirect: func(
			req *http.Request,
			via []*http.Request,
		) error {
			return http.ErrUseLastResponse
		},
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	targets := make(map[uint64]string)

	setError := func(err error) {
		mu.Lock()
		defer mu.Unlock()

		if firstErr == nil {
			firstErr = err
		}
	}

	start := time.Now()

	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()

			workload := newWorkload(cfg, worker)

			clientID := fmt.Sprintf(
				"bench-%s-worker-%d",
				runID, worker,
			)

			for i := worker; i < cfg.Requests; i += cfg.Concurrency {
				if ctx.Err() != nil {
					setError(ctx.Err())
					return
				}

				key := fmt.Sprintf(
					"bench-%s-key-%d",
					runID, workload.nextKey(),
				)

				target := cfg.Targets[i%len(cfg.Targets)]

				in := history.Input{
					ClientID: clientID,
					Kind:     history.Get,
					Key:      key,
				}

				if !workload.isRead(cfg.ReadRatio) {
					in.Kind = history.Put
					in.Value = workload.value(cfg.ValueSize)
					in.RequestID = fmt.Sprintf("request-%d", i)
				}

				id, err := recorder.Begin(in)

				if err != nil {
					setError(err)
					return
				}

				out := execute(ctx, client, target, in)

				if err := recorder.Complete(id, out); err != nil {
					setError(err)
					return
				}

				mu.Lock()
				targets[id] = target
				mu.Unlock()
			}
		}(worker)
	}

	wg.Wait()

	return RunResult{
		RunID:    runID,
		Config:   cfg,
		Duration: time.Since(start),
		History:  recorder.Snapshot(),
		Targets:  targets,
	}, firstErr
}
