package kvstore

import "errors"

// NewWithEngine creates a KV state machine with an
// optional persistent materialized-view engine.
//
// The caller owns the engine and must close it.
func NewWithEngine(engine Engine) (*Store, error) {
	if engine == nil {
		return nil, errors.New("nil KV engine")
	}

	store := New()
	store.engine = engine

	return store, nil
}
