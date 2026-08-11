package acquire

import "sync/atomic"

// roundRobin is the built-in acquire strategy.
type roundRobin struct {
	next uint32
}

// NewRoundRobin creates a strategy that cycles through shards sequentially.
//
// Behavior:
// It distributes traffic evenly irrespective of load.
//
// Concurrency Guarantees:
// Thread-safe. It uses a single atomic increment for lock-free advancement.
func NewRoundRobin() AcquireStrategy {
	return &roundRobin{}
}

// Select returns the next sequential shard index.
//
// Complexity: O(1).
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

// String identifies the strategy.
//
// Complexity: O(1).
func (r *roundRobin) String() string {
	return "RoundRobin"
}
