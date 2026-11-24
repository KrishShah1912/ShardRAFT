package raftdriver

// AppliedIndexFunc returns the actual state machine's
// applied index, not the Raft commit index.
type AppliedIndexFunc func() uint64

// SetReadStateAppliedIndex configures the state-machine
// progress provider used for read-state delivery.
//
// A nil provider disables delivery until a provider
// is configured.
//
// The caller must serialize configuration and processing.
// The provider must not reenter the driver.
func (d *Driver) SetReadStateAppliedIndex(
	provider AppliedIndexFunc,
) error {
	if d.stopped {
		return ErrStopped
	}

	d.readStateAppliedIndex = provider

	return nil
}

// readBarrierSatisfied verifies that the state machine
// has applied every entry required by the read barrier.
func (d *Driver) readBarrierSatisfied(index uint64) bool {
	if index == 0 || d.readStateAppliedIndex == nil {
		return false
	}

	return d.readStateAppliedIndex() >= index
}
