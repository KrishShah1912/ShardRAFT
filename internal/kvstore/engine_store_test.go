package kvstore

import (
	"path/filepath"
	"testing"
)

func TestNewWithEngine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pebble")

	engine, err := OpenPebble(path)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	store, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}

	if store.engine == nil {
		t.Fatal("expected attached KV engine")
	}

	if _, err := NewWithEngine(nil); err == nil {
		t.Fatal("expected nil engine error")
	}
}
