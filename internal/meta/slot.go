package meta

import "hash/crc32"

// TotalSlots is the number of logical hash slots in ShardRAFT.
//
// The key space is divided into exactly this many slots, and every
// shard owns a contiguous range of them.
const TotalSlots uint32 = 65536

// SlotForKey returns the deterministic logical slot for a key.
//
// The calculation uses CRC32 IEEE over the raw key bytes. Returned
// slots are always within the inclusive range [0, TotalSlots-1].
func SlotForKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) % TotalSlots
}
