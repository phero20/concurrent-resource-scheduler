package placement

import "sync/atomic"

// roundRobin is the built-in acquire strategy.
type roundRobin struct {
	next uint32
}

// NewRoundRobin returns a new built-in Round Robin acquire strategy.
func NewRoundRobin() AcquireStrategy {
	return &roundRobin{}
}

// Select selects the next candidate Heap Shard using a round-robin approach.
func (r *roundRobin) Select(shards ShardView) int {
	n := shards.ShardCount()
	// Defensive safeguard: ShardCount is guaranteed to be >= 1 by configuration validation,
	// but we guard against division by zero in case of an invalid mock or test view.
	if n <= 0 {
		return 0
	}
	idx := atomic.AddUint32(&r.next, 1) - 1
	return int(idx % uint32(n))
}
