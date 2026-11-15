package kvstore

import (
	"path/filepath"
	"testing"
)

func TestPebbleEnginePutGetDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pebble")

	engine, err := OpenPebble(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.Put("name", "ShardRAFT"); err != nil {
		t.Fatal(err)
	}

	value, exists, err := engine.Get("name")
	if err != nil || !exists || value != "ShardRAFT" {
		t.Fatalf("unexpected value: %q, exists=%v, err=%v",
			value, exists, err)
	}

	if err := engine.Delete("name"); err != nil {
		t.Fatal(err)
	}

	_, exists, err = engine.Get("name")
	if err != nil || exists {
		t.Fatalf("expected deleted key, exists=%v, err=%v",
			exists, err)
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPebbleEngineSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pebble")

	engine, err := OpenPebble(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.Put("counter", "42"); err != nil {
		t.Fatal(err)
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPebble(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	value, exists, err := reopened.Get("counter")
	if err != nil || !exists || value != "42" {
		t.Fatalf("restart recovery failed: %q, exists=%v, err=%v",
			value, exists, err)
	}
}
