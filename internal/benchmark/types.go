package benchmark

import "time"

type Config struct {
	Target      string        `json:"target"`
	Requests    int           `json:"requests"`
	Concurrency int           `json:"concurrency"`
	ReadRatio   float64       `json:"read_ratio"`
	ValueSize   int           `json:"value_size"`
	Timeout     time.Duration `json:"-"`
}

type NodeStatus struct {
	ID           int    `json:"id"`
	Role         int    `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     int    `json:"leader_id"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
}

type LatencyStats struct {
	AverageMS float64 `json:"average_ms"`
	P50MS     float64 `json:"p50_ms"`
	P95MS     float64 `json:"p95_ms"`
	P99MS     float64 `json:"p99_ms"`
	MaxMS     float64 `json:"max_ms"`
}

type Result struct {
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`

	Requests    int     `json:"requests"`
	Concurrency int     `json:"concurrency"`
	ReadRatio   float64 `json:"read_ratio"`
	ValueSize   int     `json:"value_size"`

	Successful int `json:"successful"`
	Failed     int `json:"failed"`
	Reads      int `json:"reads"`
	Writes     int `json:"writes"`

	DurationMS    float64 `json:"duration_ms"`
	ThroughputOPS float64 `json:"throughput_ops"`

	OverallLatency LatencyStats `json:"overall_latency"`
	ReadLatency    LatencyStats `json:"read_latency"`
	WriteLatency   LatencyStats `json:"write_latency"`

	StatusCodes map[int]int    `json:"status_codes"`
	Errors      map[string]int `json:"errors"`

	InitialLeader NodeStatus `json:"initial_leader"`
	FinalLeader   NodeStatus `json:"final_leader"`
}
