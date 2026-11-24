package wal

type SegmentedStats struct {
	ActiveSegment  uint64 `json:"active_segment"`
	ActiveSize     int64  `json:"active_size"`
	MaxSegmentSize int64  `json:"max_segment_size"`
	Recovered      bool   `json:"recovered"`
	Closed         bool   `json:"closed"`
	Unusable       bool   `json:"unusable"`
}

func (w *SegmentedWAL) Stats() SegmentedStats {
	return SegmentedStats{
		ActiveSegment:  w.number,
		ActiveSize:     w.size,
		MaxSegmentSize: w.options.MaxSegmentSize,
		Recovered:      w.recovered,
		Closed:         w.closed,
		Unusable:       w.unusable,
	}
}
