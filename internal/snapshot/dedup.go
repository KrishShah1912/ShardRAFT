package snapshot

// DedupRecord preserves the original outcome of a
// client request across snapshot recovery.
type DedupRecord struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	Expected  string `json:"expected,omitempty"`
	Applied   bool   `json:"applied"`
	Result    string `json:"result,omitempty"`
}

// NewWithDedup creates a snapshot containing KV data
// and a copy of the request deduplication history.
func NewWithDedup(
	index, term uint64,
	data map[string]string,
	dedup map[string]DedupRecord,
) Snapshot {
	s := New(index, term, data)

	s.Dedup = make(map[string]DedupRecord, len(dedup))
	for key, record := range dedup {
		s.Dedup[key] = record
	}

	return s
}
