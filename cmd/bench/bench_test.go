package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KrishShah1912/shardraft/internal/history"
)

func testConfig(t *testing.T) Config {
	t.Helper()

	cfg, err := parseConfig(nil, io.Discard)

	if err != nil {
		t.Fatal(err)
	}

	return cfg
}

func TestConfigValidation(t *testing.T) {
	for _, args := range [][]string{
		{"-requests", "0"},
		{"-keys", "0"},
		{"-read-ratio", "1.1"},
		{"-read-ratio", "NaN"},
		{"-distribution", "invalid"},
		{"-zipf-s", "1"},
		{"-value-size", "0"},
		{"-targets", "invalid"},
	} {
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Fatalf("accepted invalid config: %v", args)
		}
	}
}

func TestUniformDistribution(t *testing.T) {
	cfg := testConfig(t)
	cfg.Keys = 100

	w := newWorkload(cfg, 0)
	counts := make([]int, cfg.Keys)

	for i := 0; i < 20000; i++ {
		counts[w.nextKey()]++
	}

	for key, count := range counts {
		if count < 100 || count > 300 {
			t.Fatalf("unexpected uniform count: key=%d count=%d", key, count)
		}
	}
}

func TestZipfianDistribution(t *testing.T) {
	cfg := testConfig(t)
	cfg.Distribution = "zipfian"
	cfg.Keys = 100

	w := newWorkload(cfg, 0)
	counts := make([]int, cfg.Keys)

	for i := 0; i < 20000; i++ {
		counts[w.nextKey()]++
	}

	if counts[0] < counts[99]*10 {
		t.Fatal("Zipfian workload did not favor hot keys")
	}

	t.Logf("hottest key=%d coldest-rank key=%d", counts[0], counts[99])
}

func TestWorkloadReproducibility(t *testing.T) {
	cfg := testConfig(t)

	a := newWorkload(cfg, 2)
	b := newWorkload(cfg, 2)

	for i := 0; i < 100; i++ {
		if a.nextKey() != b.nextKey() {
			t.Fatal("seeded workload is not reproducible")
		}
	}
}

func TestValueSizeAndReadRatio(t *testing.T) {
	cfg := testConfig(t)
	w := newWorkload(cfg, 0)

	for _, size := range []int{1, 128, 1024, 4096} {
		if len(w.value(size)) != size {
			t.Fatalf("incorrect value size: %d", size)
		}
	}

	for i := 0; i < 100; i++ {
		if w.isRead(0) || !w.isRead(1) {
			t.Fatal("incorrect boundary read ratio")
		}
	}

	reads := 0

	for i := 0; i < 10000; i++ {
		if w.isRead(0.7) {
			reads++
		}
	}

	if reads < 6700 || reads > 7300 {
		t.Fatalf("unexpected read count: %d", reads)
	}
}

func TestPercentiles(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	if percentile(values, 0.50) != 5 ||
		percentile(values, 0.95) != 10 ||
		percentile(values, 0.99) != 10 {
		t.Fatal("incorrect percentile calculation")
	}
}

func TestBenchmarkEndToEnd(t *testing.T) {
	var mu sync.Mutex
	store := make(map[string]string)

	handler := http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		key := r.URL.Path[len("/kv/"):]

		mu.Lock()
		defer mu.Unlock()

		w.Header().Set("Content-Type", "application/json")

		response := gatewayResponse{}

		switch r.Method {
		case http.MethodPut:
			var input map[string]string

			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			store[key] = input["value"]

			response.Value = input["value"]
			response.Applied = true
			response.ClientID = input["client_id"]
			response.RequestID = input["request_id"]

		case http.MethodGet:
			response.Key = key
			response.Value, response.Found = store[key]

			if !response.Found {
				w.WriteHeader(http.StatusNotFound)

				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "key not found",
					"code":  "key_not_found",
				})

				return
			}

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if r.Method == http.MethodPut {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"applied":    response.Applied,
				"value":      response.Value,
				"client_id":  response.ClientID,
				"request_id": response.RequestID,
			})
			return
		}

		_ = json.NewEncoder(w).Encode(response)
	})

	first := httptest.NewServer(handler)
	defer first.Close()

	second := httptest.NewServer(handler)
	defer second.Close()

	cfg := testConfig(t)
	cfg.Targets = []string{first.URL, second.URL}
	cfg.Requests = 100
	cfg.Concurrency = 4
	cfg.Keys = 10
	cfg.ValueSize = 128
	cfg.OutputDir = t.TempDir()

	result, err := runBenchmark(context.Background(), cfg)

	if err != nil {
		t.Fatal(err)
	}

	summary := summarize(result)

	if summary.Successful != cfg.Requests ||
		summary.Reads+summary.Writes != cfg.Requests {
		t.Fatalf("unexpected summary: %+v", summary)
	}

	report, err := history.Check(
		result.History,
		history.CheckOptions{
			AssumeEmpty: true,
			Timeout:     5 * time.Second,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if report.Verdict != history.Linearizable {
		t.Fatalf("unexpected checker verdict: %+v", report)
	}

	dir, err := saveResults(result, summary)

	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{
		"history.json",
		"operations.csv",
		"summary.json",
		"config.json",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}

	file, err := os.Open(filepath.Join(dir, "operations.csv"))

	if err != nil {
		t.Fatal(err)
	}

	defer file.Close()

	rows, err := csv.NewReader(file).ReadAll()

	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != cfg.Requests+1 {
		t.Fatalf("unexpected CSV row count: %d", len(rows))
	}

	t.Logf("validated %d operations and raw CSV", cfg.Requests)
}

func TestGatewayFailurePreservation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	defer server.Close()

	client := server.Client()

	for _, kind := range []history.Kind{
		history.Get,
		history.Put,
	} {
		out := execute(
			context.Background(),
			client,
			server.URL,
			history.Input{
				Kind:      kind,
				Key:       "key",
				ClientID:  "client",
				RequestID: "request",
			},
		)

		expected := history.Failed

		if kind == history.Put {
			expected = history.Unknown
		}

		if out.Outcome != expected || out.Error == "" {
			t.Fatalf("incorrect failure classification: %+v", out)
		}
	}
}
