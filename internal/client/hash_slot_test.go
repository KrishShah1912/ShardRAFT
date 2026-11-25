package client

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestHashSlotTotalSlots(t *testing.T) {
	if TotalSlots != 65536 {
		t.Fatalf(
			"total slots=%d want=65536",
			TotalSlots,
		)
	}
}

func TestHashSlotKnownVectors(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want uint32
	}{
		{"Empty", "", 0},
		{"Hello", "hello", 42630},
		{"Alpha", "alpha", 14698},
		{"Beta", "beta", 1123},
		{"CRC32StandardVector", "123456789", 14630},
		{"Unicode", "你好", 47169},
		{"SingleCharacterA", "a", 48707},
		{"SingleCharacterB", "b", 61433},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HashSlot(tt.key)

			if got != tt.want {
				t.Fatalf(
					"HashSlot(%q)=%d want=%d",
					tt.key,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestHashSlotDeterministic(t *testing.T) {
	keys := []string{
		"alpha",
		"beta",
		"user:123",
		"session:456",
		"hello world",
		"你好",
		"🚀",
		"key\x00value",
		"",
	}

	for _, key := range keys {
		expected := HashSlot(key)

		for i := 0; i < 1000; i++ {
			if got := HashSlot(key); got != expected {
				t.Fatalf(
					"non-deterministic slot for %q",
					key,
				)
			}
		}
	}
}

func TestHashSlotRange(t *testing.T) {
	const samples = 100000

	for i := 0; i < samples; i++ {
		key := fmt.Sprintf("range-key-%d", i)
		slot := HashSlot(key)

		if slot >= TotalSlots {
			t.Fatalf(
				"key=%q slot=%d outside valid range",
				key,
				slot,
			)
		}
	}

	t.Logf(
		"validated %d keys within slot range 0-%d",
		samples,
		TotalSlots-1,
	)
}

func TestHashSlotDistribution(t *testing.T) {
	const (
		samples = 100000
		buckets = 4
	)

	var counts [buckets]int

	for i := 0; i < samples; i++ {
		key := fmt.Sprintf("distribution-key-%d", i)
		slot := HashSlot(key)

		bucket := slot / (TotalSlots / buckets)
		counts[bucket]++
	}

	expected := samples / buckets

	for i, count := range counts {
		if count < expected*9/10 ||
			count > expected*11/10 {
			t.Fatalf(
				"bucket %d count=%d expected approximately %d",
				i,
				count,
				expected,
			)
		}

		t.Logf(
			"bucket=%d range=%d-%d keys=%d",
			i,
			uint32(i)*(TotalSlots/buckets),
			uint32(i+1)*(TotalSlots/buckets)-1,
			count,
		)
	}
}

func TestHashSlotLargeKey(t *testing.T) {
	key := strings.Repeat("large-key-", 100000)

	first := HashSlot(key)
	second := HashSlot(key)

	if first >= TotalSlots {
		t.Fatalf("invalid slot: %d", first)
	}

	if first != second {
		t.Fatal("large key hashing is not deterministic")
	}
}

func TestHashSlotConcurrent(t *testing.T) {
	const (
		workers = 32
		keys    = 1000
	)

	expected := make([]uint32, keys)
	inputs := make([]string, keys)

	for i := 0; i < keys; i++ {
		inputs[i] = fmt.Sprintf("concurrent-key-%d", i)
		expected[i] = HashSlot(inputs[i])
	}

	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i, key := range inputs {
				if got := HashSlot(key); got != expected[i] {
					t.Errorf(
						"key=%q slot=%d want=%d",
						key,
						got,
						expected[i],
					)
					return
				}
			}
		}()
	}

	wg.Wait()

	t.Logf(
		"validated %d concurrent hash calculations",
		workers*keys,
	)
}

func BenchmarkHashSlot(b *testing.B) {
	const key = "user:123456789:profile"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = HashSlot(key)
	}
}
