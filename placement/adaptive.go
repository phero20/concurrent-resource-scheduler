package placement

import (
	"sync/atomic"
)

// AdaptiveStrategy probabilistically favors Heap Shards with fewer active resources.
//
// Behavior:
// It evaluates the load on all shards using the lock-free ActiveCount view
// and weights selection heavily toward the least contended shards.
//
// Concurrency Guarantees:
// Thread-safe. It does not introduce any global locks.
type AdaptiveStrategy struct {
	counter atomic.Uint64
}

// NewAdaptiveStrategy creates a strategy that dynamically balances load.
//
// Lifecycle:
// Provided to config.AcquireStrategy during initialization.
func NewAdaptiveStrategy() *AdaptiveStrategy {
	return &AdaptiveStrategy{}
}

// Select evaluates current shard active counts and returns a candidate shard.
//
// Complexity: O(H) where H is the total number of shards.
func (s *AdaptiveStrategy) Select(view ShardView) int {
	shardCount := view.ShardCount()
	if shardCount == 0 {
		return 0
	}
	if shardCount == 1 {
		return 0
	}

	// Increment sequence counter to drive the lock-free PRNG
	c := s.counter.Add(1)
	hashVal := mix(c)

	// Extract two independent pseudo-random 32-bit values from the 64-bit hash
	v1 := uint32(hashVal)
	v2 := uint32(hashVal >> 32)

	shard1 := int(v1 % uint32(shardCount))

	// Map v2 uniformly to the remaining (shardCount - 1) shards
	shard2 := int(v2 % uint32(shardCount-1))
	if shard2 >= shard1 {
		shard2++
	}

	// Compare O(1) shard-local active counts
	count1 := view.ActiveCount(shard1)
	count2 := view.ActiveCount(shard2)

	if count1 < count2 {
		return shard1
	}

	return shard2
}

// String identifies the strategy.
//
// Complexity: O(1).
func (s *AdaptiveStrategy) String() string {
	return "Adaptive"
}
