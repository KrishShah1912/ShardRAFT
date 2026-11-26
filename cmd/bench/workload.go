package main

import (
	"math/rand"
)

type Workload struct {
	random *rand.Rand
	zipf   *rand.Zipf
	keys   int
}

func newWorkload(cfg Config, worker int) *Workload {
	random := rand.New(
		rand.NewSource(cfg.Seed + int64(worker)*7919),
	)

	w := &Workload{
		random: random,
		keys:   cfg.Keys,
	}

	if cfg.Distribution == "zipfian" && cfg.Keys > 1 {
		w.zipf = rand.NewZipf(
			random,
			cfg.ZipfS,
			1,
			uint64(cfg.Keys-1),
		)
	}

	return w
}

func (w *Workload) nextKey() int {
	if w.keys == 1 {
		return 0
	}

	if w.zipf != nil {
		return int(w.zipf.Uint64())
	}

	return w.random.Intn(w.keys)
}

func (w *Workload) isRead(ratio float64) bool {
	return w.random.Float64() < ratio
}

func (w *Workload) value(size int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	data := make([]byte, size)

	for i := range data {
		data[i] = alphabet[w.random.Intn(len(alphabet))]
	}

	return string(data)
}
