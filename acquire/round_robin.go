package acquire

import "sync/atomic"

// roundRobin is the built-in default acquire strategy.
type roundRobin struct {
	next uint32
}

// NewRoundRobin creates a strategy that cycles through Heap Shards sequentially
// using a single atomic counter, distributing requests evenly across all shards
// without regard to current load.
//
// NewRoundRobin is well-suited for homogeneous resource pools where all shards
// hold resources of similar capacity and expected latency. It is the default
// strategy installed by [scheduler.New] when [config.Config.AcquireStrategy]
// is nil.
//
// For heterogeneous pools, consider [NewWeightedStrategy]. For dynamic load
// balancing, consider [NewAdaptiveStrategy].
//
// The returned strategy is safe for concurrent use by multiple goroutines.
func NewRoundRobin() AcquireStrategy {
	return &roundRobin{}
}

// Select returns the next shard index in the round-robin cycle.
// The index wraps around to 0 after reaching the last shard.
//
// Complexity: O(1). Uses a single atomic increment with no locks.
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
