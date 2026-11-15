package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

func (w *SegmentedWAL) Recover() (raft.PersistentState, error) {
	defer func() {
		if !w.recovered {
			w.unusable = true
		}
	}()

	var state raft.PersistentState

	if w.closed || w.unusable {
		return state, ErrUnusable
	}

	w.recovered = false

	manifest, hasManifest, err := readSegmentedManifest(w.path)
	if err != nil {
		return state, err
	}

	numbers, err := listSegments(w.path)
	if err != nil {
		return state, err
	}

	if hasManifest {
		if len(numbers) == 0 ||
			numbers[0] != manifest.FirstSegment {
			return state, fmt.Errorf(
				"%w: active checkpoint segment missing",
				ErrCorrupt,
			)
		}
	}

	recordCount := 0
	explicitCheckpoint := hasManifest &&
		manifest.CheckpointSegment != 0

	if explicitCheckpoint {
		if err := validateCheckpointReference(
			w.path,
			manifest,
		); err != nil {
			return state, err
		}

		state, err = readCheckpointFile(
			checkpointPath(
				w.path,
				manifest.CheckpointSegment,
			),
		)
		if err != nil {
			return state, fmt.Errorf(
				"%w: load published checkpoint: %v",
				ErrCorrupt,
				err,
			)
		}
	}

	for segmentIndex, number := range numbers {
		f, err := os.Open(segmentPath(w.path, number))
		if err != nil {
			return state, err
		}

		segmentRecords := 0

		for {
			header := make([]byte, headerSize)

			_, err := io.ReadFull(f, header)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = f.Close()
				return state, fmt.Errorf(
					"%w: incomplete segment header %d",
					ErrCorrupt, number,
				)
			}

			length := binary.LittleEndian.Uint32(header[:4])
			checksum := binary.LittleEndian.Uint32(header[4:])

			if length == 0 || length > maxRecord {
				_ = f.Close()
				return state, fmt.Errorf(
					"%w: invalid record length in segment %d",
					ErrCorrupt, number,
				)
			}

			data := make([]byte, length)

			if _, err := io.ReadFull(f, data); err != nil {
				_ = f.Close()
				return state, fmt.Errorf(
					"%w: incomplete payload in segment %d",
					ErrCorrupt, number,
				)
			}

			if crc32.ChecksumIEEE(data) != checksum {
				_ = f.Close()
				return state, fmt.Errorf(
					"%w: checksum mismatch in segment %d",
					ErrCorrupt, number,
				)
			}

			var r record

			if err := json.Unmarshal(data, &r); err != nil {
				_ = f.Close()
				return state, fmt.Errorf(
					"%w: invalid JSON in segment %d",
					ErrCorrupt, number,
				)
			}

			if hasManifest && !explicitCheckpoint &&
				recordCount == 0 &&
				r.Type != recordCheckpoint {
				_ = f.Close()
				return state, fmt.Errorf(
					"%w: manifest generation lacks checkpoint",
					ErrCorrupt,
				)
			}

			// A checkpoint in a later segment cannot be
			// replayed into an existing generation.
			//
			// If it is a valid, standalone checkpoint in
			// the final segment, it may be an unpublished
			// candidate left by an interrupted publication.
			if r.Type == recordCheckpoint &&
				(recordCount > 0 || explicitCheckpoint) {
				isFinalSegment := segmentIndex == len(numbers)-1
				isFirstRecord := segmentRecords == 0

				if !isFinalSegment || !isFirstRecord {
					_ = f.Close()
					return state, fmt.Errorf(
						"%w: unexpected checkpoint in segment %d",
						ErrCorrupt, number,
					)
				}

				// Validate the candidate independently.
				if _, err := apply(raft.PersistentState{}, r); err != nil {
					_ = f.Close()
					return state, fmt.Errorf(
						"%w: invalid unpublished checkpoint: %v",
						ErrCorrupt, err,
					)
				}

				// The candidate must be the only record in
				// this segment. Never skip a mixed segment.
				extra := make([]byte, 1)
				n, readErr := f.Read(extra)

				if n != 0 || readErr != io.EOF {
					_ = f.Close()
					return state, fmt.Errorf(
						"%w: unpublished checkpoint has trailing data",
						ErrCorrupt,
					)
				}

				if err := f.Close(); err != nil {
					return state, err
				}

				// The orphan is not part of the recovered
				// generation. Remove it before allowing
				// any new WAL writes.
				if err := os.Remove(segmentPath(w.path, number)); err != nil {
					return state, fmt.Errorf(
						"remove unpublished checkpoint: %w",
						err,
					)
				}

				if err := defaultPhysicalCompactionOps().syncDir(
					segmentDirectory(w.path),
				); err != nil {
					return state, fmt.Errorf(
						"sync WAL directory after orphan removal: %w",
						err,
					)
				}

				previousNumber := numbers[segmentIndex-1]

				previous, err := os.OpenFile(
					segmentPath(w.path, previousNumber),
					os.O_RDWR,
					0644,
				)
				if err != nil {
					return state, err
				}

				info, err := previous.Stat()
				if err != nil {
					_ = previous.Close()
					return state, err
				}

				if err := w.active.Close(); err != nil {
					_ = previous.Close()
					return state, err
				}

				w.active = previous
				w.number = previousNumber
				w.size = info.Size()

				w.state = copyState(state)
				w.recovered = true
				return copyState(state), nil
			}

			next, err := apply(state, r)
			if err != nil {
				_ = f.Close()
				return state, fmt.Errorf(
					"%w: invalid record in segment %d: %v",
					ErrCorrupt, number, err,
				)
			}

			state = next
			recordCount++
			segmentRecords++
		}

		if err := f.Close(); err != nil {
			return state, err
		}
	}

	if hasManifest && !explicitCheckpoint && recordCount == 0 {
		return state, fmt.Errorf(
			"%w: empty checkpoint generation",
			ErrCorrupt,
		)
	}

	w.state = copyState(state)
	w.recovered = true

	return copyState(state), nil
}
