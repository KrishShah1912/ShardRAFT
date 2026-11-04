package raft

type Consensus interface {
	// Advance logical time.
	Tick()

	// Process an incoming Raft message.
	Step(msg Message) error

	// Propose a new command.
	Propose(data []byte) error

	// Request a linearizable read barrier.
	ReadIndex(ctx []byte) error

	// Return pending work.
	Ready() Ready

	// Acknowledge processed Ready work.
	Advance()

	// Return current node status.
	Status() Status
}
