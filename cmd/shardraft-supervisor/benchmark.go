package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/KrishShah1912/shardraft/internal/benchmark"
)

type benchmarkState string

const (
	benchmarkIdle    benchmarkState = "idle"
	benchmarkRunning benchmarkState = "running"
	benchmarkPassed  benchmarkState = "passed"
	benchmarkFailed  benchmarkState = "failed"
)

type benchmarkRequest struct {
	Requests    int     `json:"requests"`
	Concurrency int     `json:"concurrency"`
	ReadRatio   float64 `json:"read_ratio"`
	ValueSize   int     `json:"value_size"`

	Mode string `json:"mode,omitempty"`

	FailureDelayMS int `json:"failure_delay_ms,omitempty"`
}

type benchmarkExperiment struct {
	Mode string `json:"mode"`

	FailedNodeID int `json:"failed_node_id,omitempty"`

	LeaderBefore int `json:"leader_before,omitempty"`
	LeaderAfter  int `json:"leader_after,omitempty"`

	TermBefore uint64 `json:"term_before,omitempty"`
	TermAfter  uint64 `json:"term_after,omitempty"`

	FailureStartedAt time.Time `json:"failure_started_at,omitempty"`
	LeaderElectedAt  time.Time `json:"leader_elected_at,omitempty"`
	NodeRestartedAt  time.Time `json:"node_restarted_at,omitempty"`

	FailoverMS float64 `json:"failover_ms,omitempty"`

	Recovered bool `json:"recovered"`
}

type benchmarkJob struct {
	ID string `json:"id"`

	State benchmarkState `json:"state"`

	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`

	Config benchmarkRequest `json:"config"`

	Result *benchmark.Result `json:"result,omitempty"`

	Experiment *benchmarkExperiment `json:"experiment,omitempty"`

	Error string `json:"error,omitempty"`

	PersistenceError string `json:"persistence_error,omitempty"`
}

type benchmarkManager struct {
	mu sync.RWMutex

	current *benchmarkJob
	history []benchmarkJob

	historyPath string
	supervisor  *supervisor
}

func newBenchmarkManager(
	historyPath string,
	supervisor *supervisor,
) *benchmarkManager {
	history, err :=
		loadBenchmarkHistory(
			historyPath,
		)

	if err != nil {
		history =
			[]benchmarkJob{}
	}

	return &benchmarkManager{
		history: history,

		historyPath: historyPath,

		supervisor: supervisor,
	}
}

func (
	manager *benchmarkManager,
) start(
	request benchmarkRequest,
) (benchmarkJob, error) {
	manager.mu.Lock()

	if manager.current != nil &&
		manager.current.State ==
			benchmarkRunning {

		manager.mu.Unlock()

		return benchmarkJob{},
			errors.New(
				"benchmark already running",
			)
	}

	cfg := benchmark.DefaultConfig()

	cfg.Requests =
		request.Requests

	cfg.Concurrency =
		request.Concurrency

	cfg.ReadRatio =
		request.ReadRatio

	cfg.ValueSize =
		request.ValueSize

	if err :=
		benchmark.ValidateConfig(
			cfg,
		); err != nil {

		manager.mu.Unlock()

		return benchmarkJob{},
			err
	}

	if request.Mode == "" {
		request.Mode = "normal"
	}

	switch request.Mode {
	case "normal":
	case "leader-failure":
		if request.FailureDelayMS == 0 {
			request.FailureDelayMS = 1000
		}

		if request.FailureDelayMS < 250 ||
			request.FailureDelayMS > 30000 {

			manager.mu.Unlock()

			return benchmarkJob{},
				errors.New(
					"failure delay must be between 250 and 30000 ms",
				)
		}

	default:
		manager.mu.Unlock()

		return benchmarkJob{},
			errors.New(
				"unsupported benchmark mode",
			)
	}

	now := time.Now()

	job := benchmarkJob{
		ID: now.Format(
			"20060102T150405.000000000",
		),

		State: benchmarkRunning,

		StartedAt: now,

		Config: request,
	}

	manager.current = &job

	snapshot := job

	manager.mu.Unlock()

	if request.Mode ==
		"leader-failure" {

		go manager.runLeaderFailureExperiment(
			context.Background(),
			job.ID,
			cfg,
			request,
		)
	} else {
		go manager.run(
			job.ID,
			cfg,
		)
	}

	return snapshot, nil
}

func (
	manager *benchmarkManager,
) run(
	id string,
	cfg benchmark.Config,
) {
	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			5*time.Minute,
		)

	defer cancel()

	result, err :=
		benchmark.Run(
			ctx,
			cfg,
		)

	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.current == nil ||
		manager.current.ID != id {
		return
	}

	manager.current.EndedAt =
		time.Now()

	if err != nil {
		manager.current.State =
			benchmarkFailed

		manager.current.Error =
			err.Error()
	} else {
		manager.current.State =
			benchmarkPassed

		manager.current.Result =
			&result
	}

	manager.persistCurrentLocked()
}

func (
	manager *benchmarkManager,
) currentJob() *benchmarkJob {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	if manager.current == nil {
		return nil
	}

	job := *manager.current

	return &job
}

func (
	manager *benchmarkManager,
) historySnapshot() []benchmarkJob {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	result :=
		make(
			[]benchmarkJob,
			len(manager.history),
		)

	copy(
		result,
		manager.history,
	)

	return result
}

func (
	manager *benchmarkManager,
) setExperiment(
	id string,
	experiment benchmarkExperiment,
) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.current == nil ||
		manager.current.ID != id {
		return
	}

	manager.current.Experiment =
		&experiment
}

func (
	manager *benchmarkManager,
) updateExperiment(
	id string,
	update func(
		*benchmarkExperiment,
	),
) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.current == nil ||
		manager.current.ID != id ||
		manager.current.Experiment == nil {

		return
	}

	update(
		manager.current.Experiment,
	)
}

func (
	manager *benchmarkManager,
) finishExperiment(
	id string,
	result benchmark.Result,
	runErr error,
) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.current == nil ||
		manager.current.ID != id {
		return
	}

	manager.current.EndedAt =
		time.Now()

	if runErr != nil {
		manager.current.State =
			benchmarkFailed

		manager.current.Error =
			runErr.Error()
	} else {
		manager.current.State =
			benchmarkPassed

		manager.current.Result =
			&result
	}

	manager.persistCurrentLocked()
}

func (
	manager *benchmarkManager,
) finishExperimentFailure(
	id string,
	err error,
) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.current == nil ||
		manager.current.ID != id {
		return
	}

	manager.current.EndedAt =
		time.Now()

	manager.current.State =
		benchmarkFailed

	if err != nil {
		manager.current.Error =
			err.Error()
	}

	manager.persistCurrentLocked()
}

func (
	manager *benchmarkManager,
) persistCurrentLocked() {
	if manager.current == nil {
		return
	}

	manager.history =
		append(
			[]benchmarkJob{
				*manager.current,
			},
			manager.history...,
		)

	if len(manager.history) >
		benchmarkHistoryLimit {

		manager.history =
			manager.history[:benchmarkHistoryLimit]
	}

	if err :=
		saveBenchmarkHistory(
			manager.historyPath,
			manager.history,
		); err != nil {

		manager.current.PersistenceError =
			err.Error()

		manager.history[0] =
			*manager.current
	}
}
