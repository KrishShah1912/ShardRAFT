package client

import "hash/crc32"

// TotalSlots is the number of logical hash slots in ShardRAFT.
const TotalSlots uint32 = 65536

// HashSlot returns the deterministic logical slot for a key.
//
// The calculation uses CRC32 IEEE over the raw key bytes.
// Returned slots are always within the inclusive range
// [0, TotalSlots-1].
func HashSlot(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) % TotalSlots
}
