package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type nodeConfig struct {
	ID        int
	Port      int
	Peers     string
	PeerAddrs string
}

type nodeProcess struct {
	Config    nodeConfig
	Cmd       *exec.Cmd
	PID       int
	Running   bool
	StartedAt time.Time
	LastError string
}

type nodeStatus struct {
	ID        int       `json:"id"`
	Port      int       `json:"port"`
	PID       int       `json:"pid,omitempty"`
	Running   bool      `json:"running"`
	Reachable bool      `json:"reachable"`
	StartedAt time.Time `json:"started_at,omitempty"`
	LastError string    `json:"last_error,omitempty"`
}

type supervisor struct {
	mu sync.Mutex

	root       string
	binary     string
	dataDir    string
	logDir     string
	httpClient *http.Client

	nodes map[int]*nodeProcess
}

func main() {
	var (
		addr = flag.String(
			"addr",
			"127.0.0.1:8090",
			"supervisor HTTP address",
		)

		root = flag.String(
			"root",
			".",
			"ShardRAFT repository root",
		)
	)

	flag.Parse()

	absoluteRoot, err := filepath.Abs(*root)
	if err != nil {
		log.Fatal(err)
	}

	s := newSupervisor(absoluteRoot)

	if err := s.prepare(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	benchmarkHistoryPath :=
		os.Getenv(
			"SHARDRAFT_BENCHMARK_HISTORY",
		)

	if benchmarkHistoryPath == "" {
		benchmarkHistoryPath =
			".dev-cluster/benchmark-history.json"
	}

	benchmarks :=
		newBenchmarkManager(
			benchmarkHistoryPath,
			s,
		)

	registerBenchmarkHandlers(
		mux,
		benchmarks,
	)

	mux.HandleFunc(
		"GET /api/v1/control/cluster",
		s.handleCluster,
	)

	mux.HandleFunc(
		"POST /api/v1/control/nodes/{id}/start",
		s.handleStart,
	)

	mux.HandleFunc(
		"POST /api/v1/control/nodes/{id}/stop",
		s.handleStop,
	)

	mux.HandleFunc(
		"POST /api/v1/control/nodes/{id}/restart",
		s.handleRestart,
	)

	mux.HandleFunc(
		"GET /health",
		func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{
				"status": "ok",
			})
		},
	)

	server := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf(
		"ShardRAFT supervisor listening on http://%s",
		*addr,
	)

	log.Fatal(server.ListenAndServe())
}

func newSupervisor(root string) *supervisor {
	runtimeDir := filepath.Join(root, ".dev-cluster")

	return &supervisor{
		root:    root,
		binary:  filepath.Join(runtimeDir, "shardraftd"),
		dataDir: filepath.Join(runtimeDir, "data"),
		logDir:  filepath.Join(runtimeDir, "logs"),

		httpClient: &http.Client{
			Timeout: 350 * time.Millisecond,
		},

		nodes: map[int]*nodeProcess{
			1: {
				Config: nodeConfig{
					ID:        1,
					Port:      8081,
					Peers:     "2,3",
					PeerAddrs: "2=127.0.0.1:8082,3=127.0.0.1:8083",
				},
			},

			2: {
				Config: nodeConfig{
					ID:        2,
					Port:      8082,
					Peers:     "1,3",
					PeerAddrs: "1=127.0.0.1:8081,3=127.0.0.1:8083",
				},
			},

			3: {
				Config: nodeConfig{
					ID:        3,
					Port:      8083,
					Peers:     "1,2",
					PeerAddrs: "1=127.0.0.1:8081,2=127.0.0.1:8082",
				},
			},
		},
	}
}

func (s *supervisor) prepare() error {
	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		return err
	}

	if err := os.MkdirAll(s.logDir, 0o755); err != nil {
		return err
	}

	if _, err := os.Stat(s.binary); err == nil {
		return nil
	}

	log.Println("building shardraftd for supervisor...")

	cmd := exec.Command(
		"go",
		"build",
		"-o",
		s.binary,
		"./cmd/shardraftd",
	)

	cmd.Dir = s.root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func (s *supervisor) handleCluster(
	w http.ResponseWriter,
	r *http.Request,
) {
	statuses := make([]nodeStatus, 0, 3)

	for id := 1; id <= 3; id++ {
		statuses = append(
			statuses,
			s.status(id),
		)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"nodes": statuses,
	})
}

func (s *supervisor) handleStart(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := nodeID(r)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	if err := s.start(id); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusConflict,
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		s.status(id),
	)
}

func (s *supervisor) handleStop(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := nodeID(r)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	if err := s.stop(id); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusConflict,
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		s.status(id),
	)
}

func (s *supervisor) handleRestart(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := nodeID(r)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	if err := s.stop(id); err != nil &&
		!errors.Is(err, errNotRunning) {

		http.Error(
			w,
			err.Error(),
			http.StatusConflict,
		)
		return
	}

	time.Sleep(300 * time.Millisecond)

	if err := s.start(id); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusConflict,
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		s.status(id),
	)
}

var errNotRunning = errors.New("node is not running")

func (s *supervisor) start(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("unknown node %d", id)
	}

	if node.Running &&
		node.Cmd != nil &&
		node.Cmd.Process != nil {

		if processAlive(node.Cmd.Process) {
			return fmt.Errorf(
				"node %d is already running",
				id,
			)
		}
	}

	logPath := filepath.Join(
		s.logDir,
		fmt.Sprintf("node-%d.log", id),
	)

	logFile, err := os.OpenFile(
		logPath,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		return err
	}

	config := node.Config

	cmd := exec.Command(
		s.binary,
		"-id",
		strconv.Itoa(config.ID),
		"-peers",
		config.Peers,
		"-peer-addrs",
		config.PeerAddrs,
		"-data-dir",
		filepath.Join(
			s.dataDir,
			fmt.Sprintf("node-%d", id),
		),
		"-http-addr",
		fmt.Sprintf(
			"127.0.0.1:%d",
			config.Port,
		),
		"-election-timeout",
		"10",
		"-tick-interval",
		"100ms",
	)

	cmd.Dir = s.root
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		node.LastError = err.Error()
		return err
	}

	node.Cmd = cmd
	node.PID = cmd.Process.Pid
	node.Running = true
	node.StartedAt = time.Now().UTC()
	node.LastError = ""

	go func() {
		err := cmd.Wait()
		_ = logFile.Close()

		s.mu.Lock()
		defer s.mu.Unlock()

		current := s.nodes[id]

		if current.Cmd != cmd {
			return
		}

		current.Running = false
		current.PID = 0

		if err != nil {
			current.LastError = err.Error()
		}
	}()

	return nil
}

func (s *supervisor) stop(id int) error {
	s.mu.Lock()

	node, ok := s.nodes[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("unknown node %d", id)
	}

	if !node.Running ||
		node.Cmd == nil ||
		node.Cmd.Process == nil {

		s.mu.Unlock()
		return errNotRunning
	}

	process := node.Cmd.Process
	node.Running = false
	node.PID = 0

	s.mu.Unlock()

	if err := process.Signal(syscall.SIGTERM); err != nil {
		if strings.Contains(
			strings.ToLower(err.Error()),
			"process already finished",
		) {
			return nil
		}

		return err
	}

	for range 30 {
		if !processAlive(process) {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return process.Kill()
}

func (s *supervisor) status(id int) nodeStatus {
	s.mu.Lock()

	node, ok := s.nodes[id]
	if !ok {
		s.mu.Unlock()

		return nodeStatus{
			ID:        id,
			Running:   false,
			Reachable: false,
			LastError: "unknown node",
		}
	}

	status := nodeStatus{
		ID:        node.Config.ID,
		Port:      node.Config.Port,
		PID:       node.PID,
		Running:   node.Running,
		StartedAt: node.StartedAt,
		LastError: node.LastError,
	}

	s.mu.Unlock()

	status.Reachable = s.reachable(node.Config.Port)

	return status
}

func (s *supervisor) reachable(port int) bool {
	response, err := s.httpClient.Get(
		fmt.Sprintf(
			"http://127.0.0.1:%d/status",
			port,
		),
	)

	if err != nil {
		return false
	}

	_ = response.Body.Close()

	return response.StatusCode >= 200 &&
		response.StatusCode < 500
}

func nodeID(r *http.Request) (int, error) {
	raw := r.PathValue("id")

	id, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid node ID %q",
			raw,
		)
	}

	if id < 1 || id > 3 {
		return 0, fmt.Errorf(
			"node ID must be 1, 2, or 3",
		)
	}

	return id, nil
}

func processAlive(process *os.Process) bool {
	if process == nil {
		return false
	}

	return process.Signal(syscall.Signal(0)) == nil
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}
