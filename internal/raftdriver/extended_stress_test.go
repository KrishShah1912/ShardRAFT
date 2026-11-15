//go:build stress

package raftdriver

import "testing"

// TestExtendedRaftStress runs a larger deterministic
// randomized fault workload.
//
// Run with:
// go test -tags=stress -run '^TestExtendedRaftStress$' ./internal/raftdriver
func TestExtendedRaftStress(t *testing.T) {
	runRandomizedRaftFaults(t, 100, 100)
}
