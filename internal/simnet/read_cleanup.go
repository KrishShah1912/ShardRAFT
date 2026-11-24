package simnet

// pruneObsoleteReadStates releases collected confirmations
// that no longer belong to current local leadership.
//
// Current confirmations are retained regardless of
// whether their application barrier has been satisfied.
func (c *Cluster) pruneObsoleteReadStates() {
	remaining := c.readStates[:0]

	for _, item := range c.readStates {
		if c.readStateCurrent(item) {
			remaining = append(remaining, item)
		}
	}

	clear(c.readStates[len(remaining):])
	c.readStates = remaining

	if len(c.readStates) == 0 {
		c.readStates = nil
	}
}
