package meta

import "testing"

func TestSlotForKeyIsDeterministic(t *testing.T) {
	keys := []string{
		"alpha",
		"user:123",
		"session:456",
		"",
		"你好",
		"🚀",
		"key\x00value",
	}

	for _, key := range keys {
		first := SlotForKey(key)

		for i := 0; i < 1000; i++ {
			if got := SlotForKey(key); got != first {
				t.Fatalf(
					"key %q: slot changed from %d to %d",
					key,
					first,
					got,
				)
			}
		}
	}
}

func TestSlotForKeyWithinRange(t *testing.T) {
	for i := 0; i < 100000; i++ {
		key := string(rune(i)) + ":value"

		if slot := SlotForKey(key); slot >= TotalSlots {
			t.Fatalf(
				"key %q produced out-of-range slot %d",
				key,
				slot,
			)
		}
	}
}
