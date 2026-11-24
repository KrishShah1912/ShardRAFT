package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type validationState string

const (
	validationIdle    validationState = "idle"
	validationRunning validationState = "running"
	validationPassed  validationState = "passed"
	validationFailed  validationState = "failed"
)

type validationJob struct {
	State validationState `json:"state"`

	StartedAt time.Time `json:"started_at,omitempty"`
	EndedAt   time.Time `json:"ended_at,omitempty"`

	Error string `json:"error,omitempty"`
}

type validationManager struct {
	mu sync.RWMutex

	root string

	current validationJob
}

func newValidationManager(
	root string,
) *validationManager {
	return &validationManager{
		root: root,
		current: validationJob{
			State: validationIdle,
		},
	}
}

func (
	manager *validationManager,
) snapshot() validationJob {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	return manager.current
}

func (
	manager *validationManager,
) start() error {
	manager.mu.Lock()

	if manager.current.State ==
		validationRunning {

		manager.mu.Unlock()

		return errors.New(
			"validation already running",
		)
	}

	manager.current = validationJob{
		State:     validationRunning,
		StartedAt: time.Now(),
	}

	manager.mu.Unlock()

	go manager.run()

	return nil
}

func (
	manager *validationManager,
) run() {
	script :=
		filepath.Join(
			manager.root,
			"scripts",
			"generate-validation-report.py",
		)

	cmd :=
		exec.Command(
			"python3",
			script,
			"--mode",
			"standard",
		)

	cmd.Dir =
		filepath.Join(
			manager.root,
			"web",
		)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()

	manager.mu.Lock()
	defer manager.mu.Unlock()

	manager.current.EndedAt =
		time.Now()

	if err != nil {
		manager.current.State =
			validationFailed

		manager.current.Error =
			err.Error()

		return
	}

	manager.current.State =
		validationPassed
}

func registerValidationHandlers(
	mux *http.ServeMux,
	manager *validationManager,
) {
	mux.HandleFunc(
		"GET /api/v1/validation",
		func(
			w http.ResponseWriter,
			_ *http.Request,
		) {
			writeValidationJSON(
				w,
				http.StatusOK,
				manager.snapshot(),
			)
		},
	)

	mux.HandleFunc(
		"POST /api/v1/validation",
		func(
			w http.ResponseWriter,
			_ *http.Request,
		) {
			err :=
				manager.start()

			if err != nil {
				http.Error(
					w,
					err.Error(),
					http.StatusConflict,
				)

				return
			}

			writeValidationJSON(
				w,
				http.StatusAccepted,
				manager.snapshot(),
			)
		},
	)
}

func writeValidationJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(status)

	_ = json.NewEncoder(w).
		Encode(value)
}
