package history

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Recorder struct {
	mu         sync.Mutex
	start      time.Time
	lastNS     int64
	nextID     uint64
	operations []Operation
}

func NewRecorder() *Recorder {
	return &Recorder{
		start:      time.Now(),
		operations: make([]Operation, 0),
	}
}

// timestamp must be called with the mutex held.
// Strict ordering avoids equal event timestamps.
func (r *Recorder) timestamp() int64 {
	now := time.Since(r.start).Nanoseconds()

	if now <= r.lastNS {
		now = r.lastNS + 1
	}

	r.lastNS = now

	return now
}

func (r *Recorder) Begin(input Input) (uint64, error) {
	if err := input.Validate(); err != nil {
		return 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++

	op := Operation{
		ID:     r.nextID,
		CallNS: r.timestamp(),
		Input:  input,
		Output: Output{
			Outcome: Pending,
		},
	}

	r.operations = append(r.operations, op)

	return op.ID, nil
}

func (r *Recorder) Complete(id uint64, output Output) error {
	if err := output.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if id == 0 || id > uint64(len(r.operations)) {
		return fmt.Errorf("unknown operation ID: %d", id)
	}

	op := &r.operations[id-1]

	if op.Output.Outcome != Pending {
		return fmt.Errorf("operation %d already completed", id)
	}

	op.ReturnNS = r.timestamp()
	op.Output = output

	return nil
}

func (r *Recorder) Snapshot() History {
	r.mu.Lock()
	defer r.mu.Unlock()

	operations := make([]Operation, len(r.operations))
	copy(operations, r.operations)

	return History{
		Version:    FormatVersion,
		Operations: operations,
	}
}

func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.operations)
}

func (r *Recorder) Save(path string) error {
	return Save(path, r.Snapshot())
}

func Encode(w io.Writer, h History) error {
	if err := h.Validate(); err != nil {
		return err
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")

	return encoder.Encode(h)
}

func Decode(r io.Reader) (History, error) {
	var h History

	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&h); err != nil {
		return History{}, err
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		return History{}, fmt.Errorf("unexpected trailing history data")
	}

	if err := h.Validate(); err != nil {
		return History{}, err
	}

	return h, nil
}

func Save(path string, h History) error {
	if err := h.Validate(); err != nil {
		return err
	}

	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.CreateTemp(dir, ".history-*.tmp")

	if err != nil {
		return err
	}

	tempPath := file.Name()

	defer os.Remove(tempPath)
	defer file.Close()

	if err := Encode(file, h); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	return os.Rename(tempPath, path)
}

func Load(path string) (History, error) {
	file, err := os.Open(path)

	if err != nil {
		return History{}, err
	}

	defer file.Close()

	return Decode(file)
}
