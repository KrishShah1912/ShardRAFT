package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	target      string
	requests    int
	concurrency int
	readRatio   float64
	valueSize   int
	timeout     time.Duration
}

type nodeStatus struct {
	ID           int    `json:"id"`
	Role         int    `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     int    `json:"leader_id"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
}

type putRequest struct {
	Value     string `json:"value"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

type operationResult struct {
	kind       string
	latency    time.Duration
	success    bool
	statusCode int
	err        error
}

type benchmarkResult struct {
	Total          int
	Successful     int
	Failed         int
	Reads          int
	Writes         int
	ReadSuccess    int
	WriteSuccess   int
	Duration       time.Duration
	Latencies      []time.Duration
	ReadLatencies  []time.Duration
	WriteLatencies []time.Duration
	StatusCodes    map[int]int
	Errors         map[string]int
}

func main() {
	cfg := parseFlags()

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	client := &http.Client{
		Timeout: cfg.timeout,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.concurrency * 2,
			MaxIdleConnsPerHost: cfg.concurrency,
			MaxConnsPerHost:     cfg.concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	fmt.Println()
	fmt.Println("ShardRAFT Benchmark")
	fmt.Println("===================")
	fmt.Println()
	fmt.Printf("Requested target : %s\n", cfg.target)
	fmt.Println("Discovering leader...")

	leaderURL, status, err := discoverLeader(client, cfg.target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "leader discovery failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Leader           : Node %d\n", status.ID)
	fmt.Printf("Leader URL       : %s\n", leaderURL)
	fmt.Printf("Term             : %d\n", status.Term)
	fmt.Printf("Commit index     : %d\n", status.CommitIndex)
	fmt.Printf("Applied index    : %d\n", status.AppliedIndex)

	fmt.Println()
	fmt.Println("Preparing benchmark data...")

	value := strings.Repeat("x", cfg.valueSize)

	keys, err := seedKeys(
		client,
		leaderURL,
		cfg.concurrency,
		value,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed benchmark data: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Prepared         : %d keys\n", len(keys))

	fmt.Println()
	fmt.Println("Workload")
	fmt.Println("--------")
	fmt.Printf("Requests         : %d\n", cfg.requests)
	fmt.Printf("Concurrency      : %d\n", cfg.concurrency)
	fmt.Printf("Read ratio       : %.1f%%\n", cfg.readRatio*100)
	fmt.Printf("Write ratio      : %.1f%%\n", (1-cfg.readRatio)*100)
	fmt.Printf("Value size       : %d bytes\n", cfg.valueSize)
	fmt.Println()

	result := runBenchmark(
		client,
		leaderURL,
		cfg,
		keys,
		value,
	)

	printResult(result)

	finalStatus, err := fetchStatus(client, leaderURL)
	if err == nil {
		fmt.Println()
		fmt.Println("Final Cluster State")
		fmt.Println("-------------------")
		fmt.Printf("Leader           : Node %d\n", finalStatus.ID)
		fmt.Printf("Term             : %d\n", finalStatus.Term)
		fmt.Printf("Commit index     : %d\n", finalStatus.CommitIndex)
		fmt.Printf("Applied index    : %d\n", finalStatus.AppliedIndex)
	}

	if result.Failed > 0 {
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config

	flag.StringVar(
		&cfg.target,
		"target",
		"http://127.0.0.1:8081",
		"any ShardRAFT node URL",
	)

	flag.IntVar(
		&cfg.requests,
		"requests",
		10000,
		"number of operations",
	)

	flag.IntVar(
		&cfg.concurrency,
		"concurrency",
		32,
		"number of concurrent workers",
	)

	flag.Float64Var(
		&cfg.readRatio,
		"read-ratio",
		0.80,
		"fraction of operations that are reads",
	)

	flag.IntVar(
		&cfg.valueSize,
		"value-size",
		128,
		"value size in bytes",
	)

	flag.DurationVar(
		&cfg.timeout,
		"timeout",
		5*time.Second,
		"HTTP request timeout",
	)

	flag.Parse()

	cfg.target = normalizeURL(cfg.target)

	return cfg
}

func validateConfig(cfg config) error {
	if cfg.requests <= 0 {
		return errors.New("requests must be greater than zero")
	}

	if cfg.concurrency <= 0 {
		return errors.New("concurrency must be greater than zero")
	}

	if cfg.readRatio < 0 || cfg.readRatio > 1 {
		return errors.New("read-ratio must be between 0 and 1")
	}

	if cfg.valueSize < 0 {
		return errors.New("value-size cannot be negative")
	}

	if cfg.target == "" {
		return errors.New("target cannot be empty")
	}

	return nil
}

func normalizeURL(value string) string {
	return strings.TrimRight(value, "/")
}

func discoverLeader(
	client *http.Client,
	target string,
) (string, nodeStatus, error) {
	candidates := candidateURLs(target)
	var lastErr error

	for attempt := 0; attempt < 30; attempt++ {
		for _, candidate := range candidates {
			status, err := fetchStatus(client, candidate)
			if err != nil {
				lastErr = err
				continue
			}

			if status.Role == 2 && status.LeaderID == status.ID {
				return candidate, status, nil
			}

			if status.LeaderID >= 1 && status.LeaderID <= 3 {
				leaderURL := nodeURL(status.LeaderID)

				leaderStatus, err := fetchStatus(client, leaderURL)
				if err == nil &&
					leaderStatus.Role == 2 &&
					leaderStatus.LeaderID == leaderStatus.ID {
					return leaderURL, leaderStatus, nil
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	if lastErr == nil {
		lastErr = errors.New("no elected leader observed")
	}

	return "", nodeStatus{}, lastErr
}

func candidateURLs(target string) []string {
	result := []string{target}

	for id := 1; id <= 3; id++ {
		url := nodeURL(id)

		alreadyPresent := false
		for _, existing := range result {
			if existing == url {
				alreadyPresent = true
				break
			}
		}

		if !alreadyPresent {
			result = append(result, url)
		}
	}

	return result
}

func nodeURL(id int) string {
	return fmt.Sprintf(
		"http://127.0.0.1:%d",
		8080+id,
	)
}

func fetchStatus(
	client *http.Client,
	target string,
) (nodeStatus, error) {
	response, err := client.Get(target + "/status")
	if err != nil {
		return nodeStatus{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nodeStatus{}, fmt.Errorf(
			"status endpoint returned %s",
			response.Status,
		)
	}

	var status nodeStatus

	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		return nodeStatus{}, err
	}

	return status, nil
}

func seedKeys(
	client *http.Client,
	target string,
	count int,
	value string,
) ([]string, error) {
	if count < 16 {
		count = 16
	}

	if count > 256 {
		count = 256
	}

	keys := make([]string, count)
	runID := time.Now().UnixNano()

	for index := range keys {
		key := fmt.Sprintf(
			"bench:seed:%d:%d",
			runID,
			index,
		)

		keys[index] = key

		err := put(
			client,
			target,
			key,
			value,
			fmt.Sprintf(
				"seed-%d-%d",
				runID,
				index,
			),
		)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func runBenchmark(
	client *http.Client,
	target string,
	cfg config,
	keys []string,
	value string,
) benchmarkResult {
	jobs := make(chan int)
	results := make(chan operationResult, cfg.concurrency*2)

	var sequence atomic.Uint64
	var workers sync.WaitGroup

	runID := time.Now().UnixNano()

	for worker := 0; worker < cfg.concurrency; worker++ {
		workers.Add(1)

		go func(workerID int) {
			defer workers.Done()

			for operation := range jobs {
				isRead := operationIsRead(
					operation,
					cfg.readRatio,
				)

				keyIndex := operation % len(keys)
				key := keys[keyIndex]

				started := time.Now()

				var statusCode int
				var err error

				if isRead {
					statusCode, err = get(
						client,
						target,
						key,
					)
				} else {
					sequenceID := sequence.Add(1)

					statusCode, err = putWithStatus(
						client,
						target,
						key,
						value,
						fmt.Sprintf(
							"bench-%d-%d-%d-%d",
							runID,
							workerID,
							operation,
							sequenceID,
						),
					)
				}

				kind := "write"
				if isRead {
					kind = "read"
				}

				results <- operationResult{
					kind:       kind,
					latency:    time.Since(started),
					success:    err == nil,
					statusCode: statusCode,
					err:        err,
				}
			}
		}(worker)
	}

	started := time.Now()

	go func() {
		for operation := 0; operation < cfg.requests; operation++ {
			jobs <- operation
		}

		close(jobs)

		workers.Wait()
		close(results)
	}()

	result := benchmarkResult{
		Total:       cfg.requests,
		StatusCodes: make(map[int]int),
		Errors:      make(map[string]int),
	}

	for operation := range results {
		result.Latencies = append(
			result.Latencies,
			operation.latency,
		)

		if operation.kind == "read" {
			result.Reads++
			result.ReadLatencies = append(
				result.ReadLatencies,
				operation.latency,
			)
		} else {
			result.Writes++
			result.WriteLatencies = append(
				result.WriteLatencies,
				operation.latency,
			)
		}

		if operation.statusCode != 0 {
			result.StatusCodes[operation.statusCode]++
		}

		if operation.success {
			result.Successful++

			if operation.kind == "read" {
				result.ReadSuccess++
			} else {
				result.WriteSuccess++
			}
		} else {
			result.Failed++

			if operation.err != nil {
				result.Errors[operation.err.Error()]++
			}
		}
	}

	result.Duration = time.Since(started)

	return result
}

func operationIsRead(
	index int,
	ratio float64,
) bool {
	if ratio <= 0 {
		return false
	}

	if ratio >= 1 {
		return true
	}

	const scale = 10000

	threshold := int(math.Round(ratio * scale))
	slot := (index * 7919) % scale

	return slot < threshold
}

func get(
	client *http.Client,
	target string,
	key string,
) (int, error) {
	response, err := client.Get(
		target + "/kv/" + key,
	)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()

	_, _ = io.Copy(io.Discard, response.Body)

	if response.StatusCode != http.StatusOK {
		return response.StatusCode, fmt.Errorf(
			"GET returned %s",
			response.Status,
		)
	}

	return response.StatusCode, nil
}

func put(
	client *http.Client,
	target string,
	key string,
	value string,
	requestID string,
) error {
	_, err := putWithStatus(
		client,
		target,
		key,
		value,
		requestID,
	)

	return err
}

func putWithStatus(
	client *http.Client,
	target string,
	key string,
	value string,
	requestID string,
) (int, error) {
	payload, err := json.Marshal(
		putRequest{
			Value:     value,
			ClientID:  "shardraft-bench",
			RequestID: requestID,
		},
	)
	if err != nil {
		return 0, err
	}

	request, err := http.NewRequest(
		http.MethodPut,
		target+"/kv/"+key,
		bytes.NewReader(payload),
	)
	if err != nil {
		return 0, err
	}

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()

	body, _ := io.ReadAll(response.Body)

	if response.StatusCode < 200 ||
		response.StatusCode >= 300 {
		return response.StatusCode, fmt.Errorf(
			"PUT returned %s: %s",
			response.Status,
			strings.TrimSpace(string(body)),
		)
	}

	return response.StatusCode, nil
}

func printResult(result benchmarkResult) {
	fmt.Println("Results")
	fmt.Println("-------")
	fmt.Printf("Successful       : %d\n", result.Successful)
	fmt.Printf("Failed           : %d\n", result.Failed)
	fmt.Printf("Reads            : %d\n", result.Reads)
	fmt.Printf("Writes           : %d\n", result.Writes)
	fmt.Printf(
		"Elapsed          : %s\n",
		result.Duration.Round(time.Millisecond),
	)

	seconds := result.Duration.Seconds()

	if seconds > 0 {
		fmt.Printf(
			"Throughput       : %.2f ops/sec\n",
			float64(result.Successful)/seconds,
		)
	}

	fmt.Println()
	fmt.Println("Overall Latency")
	fmt.Println("---------------")
	printLatency(result.Latencies)

	fmt.Println()
	fmt.Println("Read Latency")
	fmt.Println("------------")
	printLatency(result.ReadLatencies)

	fmt.Println()
	fmt.Println("Write Latency")
	fmt.Println("-------------")
	printLatency(result.WriteLatencies)

	if len(result.Errors) > 0 {
		fmt.Println()
		fmt.Println("Failures")
		fmt.Println("--------")

		errors := make([]string, 0, len(result.Errors))

		for message := range result.Errors {
			errors = append(errors, message)
		}

		sort.Strings(errors)

		for _, message := range errors {
			fmt.Printf(
				"%d x %s\n",
				result.Errors[message],
				message,
			)
		}
	}

	if len(result.StatusCodes) > 0 {
		fmt.Println()
		fmt.Println("HTTP Status Codes")
		fmt.Println("-----------------")

		codes := make(
			[]int,
			0,
			len(result.StatusCodes),
		)

		for code := range result.StatusCodes {
			codes = append(codes, code)
		}

		sort.Ints(codes)

		for _, code := range codes {
			fmt.Printf(
				"%d              : %d\n",
				code,
				result.StatusCodes[code],
			)
		}
	}
}

func printLatency(values []time.Duration) {
	if len(values) == 0 {
		fmt.Println("No samples")
		return
	}

	sorted := append(
		[]time.Duration(nil),
		values...,
	)

	sort.Slice(
		sorted,
		func(i, j int) bool {
			return sorted[i] < sorted[j]
		},
	)

	var total time.Duration

	for _, value := range sorted {
		total += value
	}

	average := total / time.Duration(len(sorted))

	fmt.Printf(
		"Average          : %s\n",
		formatDuration(average),
	)
	fmt.Printf(
		"p50              : %s\n",
		formatDuration(percentile(sorted, 0.50)),
	)
	fmt.Printf(
		"p95              : %s\n",
		formatDuration(percentile(sorted, 0.95)),
	)
	fmt.Printf(
		"p99              : %s\n",
		formatDuration(percentile(sorted, 0.99)),
	)
	fmt.Printf(
		"Max              : %s\n",
		formatDuration(sorted[len(sorted)-1]),
	)
}

func percentile(
	sorted []time.Duration,
	p float64,
) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	if p <= 0 {
		return sorted[0]
	}

	if p >= 1 {
		return sorted[len(sorted)-1]
	}

	position := p * float64(len(sorted))
	index := int(math.Ceil(position)) - 1

	if index < 0 {
		index = 0
	}

	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

func formatDuration(value time.Duration) string {
	return fmt.Sprintf(
		"%.3f ms",
		float64(value)/
			float64(time.Millisecond),
	)
}
