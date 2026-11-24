package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type operationResult struct {
	kind       string
	latency    time.Duration
	success    bool
	statusCode int
	err        error
}

type putRequest struct {
	Value     string `json:"value"`
	ClientID  string `json:"client_id"`
	RequestID string `json:"request_id"`
}

type runResult struct {
	successful int
	failed     int
	reads      int
	writes     int

	duration time.Duration

	latencies      []time.Duration
	readLatencies  []time.Duration
	writeLatencies []time.Duration

	statusCodes map[int]int
	errors      map[string]int
}

func Run(
	ctx context.Context,
	cfg Config,
) (Result, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}

	cfg.Target =
		strings.TrimRight(
			cfg.Target,
			"/",
		)

	if err := ValidateConfig(cfg); err != nil {
		return Result{}, err
	}

	client := &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency * 2,
			MaxIdleConnsPerHost: cfg.Concurrency,
			MaxConnsPerHost:     cfg.Concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	started := time.Now()

	leaderURL,
		initialLeader,
		err :=
		discoverLeader(
			ctx,
			client,
			cfg.Target,
		)

	if err != nil {
		return Result{}, fmt.Errorf(
			"discover leader: %w",
			err,
		)
	}

	value :=
		strings.Repeat(
			"x",
			cfg.ValueSize,
		)

	keys, err := seedKeys(
		ctx,
		client,
		leaderURL,
		cfg.Concurrency,
		value,
	)

	if err != nil {
		return Result{}, fmt.Errorf(
			"seed benchmark data: %w",
			err,
		)
	}

	run, err := runWorkload(
		ctx,
		client,
		leaderURL,
		cfg,
		keys,
		value,
	)

	if err != nil {
		return Result{}, err
	}

	finalLeader := initialLeader

	if status, statusErr :=
		fetchStatus(
			ctx,
			client,
			leaderURL,
		); statusErr == nil {
		finalLeader = status
	}

	ended := time.Now()

	throughput := 0.0

	if run.duration > 0 {
		throughput =
			float64(run.successful) /
				run.duration.Seconds()
	}

	return Result{
		StartedAt: started,
		EndedAt:   ended,

		Requests:    cfg.Requests,
		Concurrency: cfg.Concurrency,
		ReadRatio:   cfg.ReadRatio,
		ValueSize:   cfg.ValueSize,

		Successful: run.successful,
		Failed:     run.failed,
		Reads:      run.reads,
		Writes:     run.writes,

		DurationMS:    durationMS(run.duration),
		ThroughputOPS: throughput,

		OverallLatency: latencyStats(
			run.latencies,
		),

		ReadLatency: latencyStats(
			run.readLatencies,
		),

		WriteLatency: latencyStats(
			run.writeLatencies,
		),

		StatusCodes: run.statusCodes,

		Errors: run.errors,

		InitialLeader: initialLeader,

		FinalLeader: finalLeader,
	}, nil
}

func discoverLeader(
	ctx context.Context,
	client *http.Client,
	target string,
) (
	string,
	NodeStatus,
	error,
) {
	candidates :=
		candidateURLs(target)

	var lastErr error

	for attempt := 0; attempt < 30; attempt++ {

		for _, candidate := range candidates {

			status, err :=
				fetchStatus(
					ctx,
					client,
					candidate,
				)

			if err != nil {
				lastErr = err
				continue
			}

			if status.Role == 2 &&
				status.LeaderID ==
					status.ID {
				return candidate,
					status,
					nil
			}

			if status.LeaderID >= 1 &&
				status.LeaderID <= 3 {

				leaderURL :=
					nodeURL(
						status.LeaderID,
					)

				leaderStatus,
					err :=
					fetchStatus(
						ctx,
						client,
						leaderURL,
					)

				if err == nil &&
					leaderStatus.Role == 2 &&
					leaderStatus.LeaderID ==
						leaderStatus.ID {

					return leaderURL,
						leaderStatus,
						nil
				}
			}
		}

		select {
		case <-ctx.Done():
			return "",
				NodeStatus{},
				ctx.Err()

		case <-time.After(
			100 * time.Millisecond,
		):
		}
	}

	if lastErr == nil {
		lastErr =
			errors.New(
				"no elected leader observed",
			)
	}

	return "",
		NodeStatus{},
		lastErr
}

func candidateURLs(
	target string,
) []string {
	result :=
		[]string{target}

	for id := 1; id <= 3; id++ {
		url := nodeURL(id)

		found := false

		for _, existing := range result {
			if existing == url {
				found = true
				break
			}
		}

		if !found {
			result =
				append(
					result,
					url,
				)
		}
	}

	return result
}

func nodeURL(
	id int,
) string {
	return fmt.Sprintf(
		"http://127.0.0.1:%d",
		8080+id,
	)
}

func fetchStatus(
	ctx context.Context,
	client *http.Client,
	target string,
) (NodeStatus, error) {
	request, err :=
		http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			target+"/status",
			nil,
		)

	if err != nil {
		return NodeStatus{}, err
	}

	response, err :=
		client.Do(request)

	if err != nil {
		return NodeStatus{}, err
	}

	defer response.Body.Close()

	if response.StatusCode !=
		http.StatusOK {
		return NodeStatus{},
			fmt.Errorf(
				"status endpoint returned %s",
				response.Status,
			)
	}

	var status NodeStatus

	if err :=
		json.NewDecoder(
			response.Body,
		).Decode(&status); err != nil {
		return NodeStatus{}, err
	}

	return status, nil
}

func seedKeys(
	ctx context.Context,
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

	keys :=
		make([]string, count)

	runID :=
		time.Now().UnixNano()

	for index := range keys {
		key := fmt.Sprintf(
			"bench:seed:%d:%d",
			runID,
			index,
		)

		keys[index] = key

		err := put(
			ctx,
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

func runWorkload(
	ctx context.Context,
	client *http.Client,
	target string,
	cfg Config,
	keys []string,
	value string,
) (runResult, error) {
	jobs :=
		make(chan int)

	results :=
		make(
			chan operationResult,
			cfg.Concurrency*2,
		)

	var sequence atomic.Uint64
	var workers sync.WaitGroup

	runID :=
		time.Now().UnixNano()

	for worker := 0; worker < cfg.Concurrency; worker++ {

		workers.Add(1)

		go func(
			workerID int,
		) {
			defer workers.Done()

			for operation := range jobs {

				if ctx.Err() != nil {
					return
				}

				isRead :=
					operationIsRead(
						operation,
						cfg.ReadRatio,
					)

				key :=
					keys[operation%len(keys)]

				started :=
					time.Now()

				var statusCode int
				var err error

				if isRead {
					statusCode,
						err =
						get(
							ctx,
							client,
							target,
							key,
						)
				} else {
					sequenceID :=
						sequence.Add(1)

					statusCode,
						err =
						putWithStatus(
							ctx,
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
					kind: kind,
					latency: time.Since(
						started,
					),
					success:    err == nil,
					statusCode: statusCode,
					err:        err,
				}
			}
		}(worker)
	}

	started :=
		time.Now()

	go func() {
		defer close(jobs)

		for operation := 0; operation <
			cfg.Requests; operation++ {

			select {
			case <-ctx.Done():
				return

			case jobs <- operation:
			}
		}
	}()

	go func() {
		workers.Wait()
		close(results)
	}()

	result := runResult{
		statusCodes: make(map[int]int),
		errors:      make(map[string]int),
	}

	for operation := range results {

		result.latencies =
			append(
				result.latencies,
				operation.latency,
			)

		if operation.kind ==
			"read" {

			result.reads++

			result.readLatencies =
				append(
					result.readLatencies,
					operation.latency,
				)
		} else {
			result.writes++

			result.writeLatencies =
				append(
					result.writeLatencies,
					operation.latency,
				)
		}

		if operation.statusCode != 0 {
			result.statusCodes[operation.statusCode]++
		}

		if operation.success {
			result.successful++
		} else {
			result.failed++

			if operation.err != nil {
				result.errors[operation.err.Error()]++
			}
		}
	}

	result.duration =
		time.Since(started)

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	return result, nil
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

	threshold :=
		int(
			math.Round(
				ratio * scale,
			),
		)

	slot :=
		(index * 7919) % scale

	return slot < threshold
}

func get(
	ctx context.Context,
	client *http.Client,
	target string,
	key string,
) (int, error) {
	request, err :=
		http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			target+"/kv/"+key,
			nil,
		)

	if err != nil {
		return 0, err
	}

	response, err :=
		client.Do(request)

	if err != nil {
		return 0, err
	}

	defer response.Body.Close()

	_, _ =
		io.Copy(
			io.Discard,
			response.Body,
		)

	if response.StatusCode !=
		http.StatusOK {

		return response.StatusCode,
			fmt.Errorf(
				"GET returned %s",
				response.Status,
			)
	}

	return response.StatusCode,
		nil
}

func put(
	ctx context.Context,
	client *http.Client,
	target string,
	key string,
	value string,
	requestID string,
) error {
	_, err :=
		putWithStatus(
			ctx,
			client,
			target,
			key,
			value,
			requestID,
		)

	return err
}

func putWithStatus(
	ctx context.Context,
	client *http.Client,
	target string,
	key string,
	value string,
	requestID string,
) (int, error) {
	payload, err :=
		json.Marshal(
			putRequest{
				Value:     value,
				ClientID:  "shardraft-bench",
				RequestID: requestID,
			},
		)

	if err != nil {
		return 0, err
	}

	request, err :=
		http.NewRequestWithContext(
			ctx,
			http.MethodPut,
			target+"/kv/"+key,
			bytes.NewReader(
				payload,
			),
		)

	if err != nil {
		return 0, err
	}

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response, err :=
		client.Do(request)

	if err != nil {
		return 0, err
	}

	defer response.Body.Close()

	body, _ :=
		io.ReadAll(
			response.Body,
		)

	if response.StatusCode < 200 ||
		response.StatusCode >= 300 {

		return response.StatusCode,
			fmt.Errorf(
				"PUT returned %s: %s",
				response.Status,
				strings.TrimSpace(
					string(body),
				),
			)
	}

	return response.StatusCode,
		nil
}
