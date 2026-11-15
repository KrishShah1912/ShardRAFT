package kvstore

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

type PebbleEngine struct {
	db *pebble.DB
}

var _ Engine = (*PebbleEngine)(nil)

func OpenPebble(path string) (*PebbleEngine, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	return &PebbleEngine{db: db}, nil
}

func (p *PebbleEngine) Put(key, value string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	return p.db.Set(
		[]byte(key),
		[]byte(value),
		pebble.Sync,
	)
}

func (p *PebbleEngine) Get(key string) (string, bool, error) {
	if key == "" {
		return "", false, errors.New("key cannot be empty")
	}

	value, closer, err := p.db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer closer.Close()

	return string(value), true, nil
}

func (p *PebbleEngine) Delete(key string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	return p.db.Delete([]byte(key), pebble.Sync)
}

func (p *PebbleEngine) Close() error {
	return p.db.Close()
}

// Clear removes all keys from the Pebble materialized view.
func (p *PebbleEngine) Clear() error {
	iter, err := p.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := p.db.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	return batch.Commit(pebble.Sync)
}
