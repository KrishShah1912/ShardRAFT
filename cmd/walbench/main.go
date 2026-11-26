package main

import (
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type measurement struct {
	mode       string
	durability string
	records    int
	recordSize int
	syncs      int
	elapsed    time.Duration
	latencies  []float64
}

func percentile(values []float64, p float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	index := int(math.Ceil(
		p*float64(len(sorted)),
	)) - 1

	if index < 0 {
		index = 0
	}

	return sorted[index]
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func measure(
	dir string,
	mode string,
	records int,
	recordSize int,
	interval time.Duration,
) (measurement, error) {
	result := measurement{
		mode:       mode,
		durability: "weaker",
		records:    records,
		recordSize: recordSize,
		latencies:  make([]float64, 0, records),
	}

	if mode == "always" {
		result.durability = "stronger"
	}

	file, err := os.CreateTemp(dir, mode+"-*.wal")
	if err != nil {
		return result, err
	}

	defer os.Remove(file.Name())
	defer file.Close()

	payload := make([]byte, recordSize)

	for i := range payload {
		payload[i] = byte(i % 251)
	}

	header := make([]byte, 8)

	binary.LittleEndian.PutUint32(
		header[:4], uint32(len(payload)),
	)

	started := time.Now()
	lastSync := started
	dirty := false

	syncFile := func() error {
		if err := file.Sync(); err != nil {
			return err
		}

		result.syncs++
		lastSync = time.Now()
		dirty = false
		return nil
	}

	for i := 0; i < records; i++ {
		opStarted := time.Now()

		binary.LittleEndian.PutUint64(
			payload[:8], uint64(i),
		)

		binary.LittleEndian.PutUint32(
			header[4:],
			crc32.ChecksumIEEE(payload),
		)

		if err := writeFull(file, header); err != nil {
			return result, err
		}

		if err := writeFull(file, payload); err != nil {
			return result, err
		}

		dirty = true

		shouldSync := mode == "always" ||
			(mode == "timed" &&
				time.Since(lastSync) >= interval)

		if shouldSync {
			if err := syncFile(); err != nil {
				return result, err
			}
		}

		result.latencies = append(
			result.latencies,
			float64(time.Since(opStarted))/
				float64(time.Millisecond),
		)
	}

	// Timed mode flushes outstanding records at
	// the end of the workload. No-sync does not.
	if mode == "timed" && dirty {
		if err := syncFile(); err != nil {
			return result, err
		}
	}

	if err := file.Close(); err != nil {
		return result, err
	}

	result.elapsed = time.Since(started)

	info, err := os.Stat(file.Name())
	if err != nil {
		return result, err
	}

	expected := int64(records) *
		int64(recordSize+8)

	if info.Size() != expected {
		return result, fmt.Errorf(
			"unexpected WAL size: got %d want %d",
			info.Size(), expected,
		)
	}

	return result, nil
}

func decimal(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func run() error {
	records := flag.Int(
		"records", 10000, "records per mode",
	)

	recordSize := flag.Int(
		"record-size", 1024, "payload bytes per record",
	)

	interval := flag.Duration(
		"sync-interval", 10*time.Millisecond,
		"timed sync interval",
	)

	output := flag.String(
		"output", "results", "results directory",
	)

	flag.Parse()

	if *records <= 0 ||
		*recordSize < 8 ||
		*recordSize > 1<<20 ||
		*interval <= 0 {
		return fmt.Errorf("invalid benchmark configuration")
	}

	dir := filepath.Join(
		*output,
		fmt.Sprintf(
			"wal-sync-%d",
			time.Now().UnixNano(),
		),
	)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	summary, err := os.Create(
		filepath.Join(dir, "wal-sync-summary.csv"),
	)
	if err != nil {
		return err
	}
	defer summary.Close()

	raw, err := os.Create(
		filepath.Join(dir, "wal-sync-operations.csv"),
	)
	if err != nil {
		return err
	}
	defer raw.Close()

	sw := csv.NewWriter(summary)
	rw := csv.NewWriter(raw)

	if err := sw.Write([]string{
		"mode",
		"durability",
		"records",
		"record_size_bytes",
		"sync_interval_ms",
		"sync_calls",
		"elapsed_ms",
		"throughput_records_sec",
		"p50_ms",
		"p95_ms",
		"p99_ms",
	}); err != nil {
		return err
	}

	if err := rw.Write([]string{
		"mode",
		"record_id",
		"latency_ms",
	}); err != nil {
		return err
	}

	fmt.Println("Isolated WAL Sync Microbenchmark")
	fmt.Println("================================")
	fmt.Println("Not a distributed Raft benchmark")
	fmt.Println("Results directory:", dir)

	for _, mode := range []string{
		"always", "timed", "no-sync",
	} {
		m, err := measure(
			dir, mode, *records,
			*recordSize, *interval,
		)
		if err != nil {
			return err
		}

		throughput := float64(m.records) /
			m.elapsed.Seconds()

		p50 := percentile(m.latencies, 0.50)
		p95 := percentile(m.latencies, 0.95)
		p99 := percentile(m.latencies, 0.99)

		intervalMS := ""

		if mode == "timed" {
			intervalMS = decimal(
				float64(*interval) /
					float64(time.Millisecond),
			)
		}

		if err := sw.Write([]string{
			mode,
			m.durability,
			strconv.Itoa(m.records),
			strconv.Itoa(m.recordSize),
			intervalMS,
			strconv.Itoa(m.syncs),
			decimal(float64(m.elapsed) /
				float64(time.Millisecond)),
			decimal(throughput),
			decimal(p50),
			decimal(p95),
			decimal(p99),
		}); err != nil {
			return err
		}

		for i, latency := range m.latencies {
			if err := rw.Write([]string{
				mode,
				strconv.Itoa(i + 1),
				decimal(latency),
			}); err != nil {
				return err
			}
		}

		fmt.Printf(
			"\nMode: %s (%s durability)\n",
			mode, m.durability,
		)

		fmt.Printf("Records: %d\n", m.records)
		fmt.Printf("Sync calls: %d\n", m.syncs)
		fmt.Printf("Elapsed: %s\n", m.elapsed)

		fmt.Printf(
			"Throughput: %.2f records/sec\n",
			throughput,
		)

		fmt.Printf("p50: %.6f ms\n", p50)
		fmt.Printf("p95: %.6f ms\n", p95)
		fmt.Printf("p99: %.6f ms\n", p99)
	}

	sw.Flush()
	rw.Flush()

	if err := sw.Error(); err != nil {
		return err
	}

	if err := rw.Error(); err != nil {
		return err
	}

	if err := summary.Sync(); err != nil {
		return err
	}

	return raw.Sync()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
