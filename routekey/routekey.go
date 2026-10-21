// Package routekey implements the stable session-to-shard mapping shared by
// the Chorus server and SDK.
//
// The hash algorithm is part of the wire contract: changing it would remap
// every existing session to a different shard.
package routekey

import "hash/fnv"

// Hash maps a session ID into the 64-bit routing space using FNV-1a.
func Hash(sid string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sid))
	return h.Sum64()
}

// Contains reports whether h falls in the half-open range [lo, hi).
//
// A hi value of 0 is a sentinel for 2^64, so [0, 0) covers the full space and
// [lo, 0) covers the unbounded tail.
func Contains(lo, hi, h uint64) bool {
	if hi == 0 {
		return h >= lo
	}
	return h >= lo && h < hi
}
