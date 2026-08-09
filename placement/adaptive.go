package placement

import (
	"sync/atomic"
)

// AdaptiveStrategy implements AcquireStrategy using the "Power of Two Choices" (P2C)
// load balancing algorithm. It probabilistically favors less-contended shards
// without requiring global state or O(H) locking overhead.
type AdaptiveStrategy struct {
	counter atomic.Uint64
}

// NewAdaptiveStrategy creates a new lock-free adaptive load balancer.
func NewAdaptiveStrategy() *AdaptiveStrategy {
	return &AdaptiveStrategy{}
}

// Select randomly chooses two shards and returns the one with the fewest active resources.
// It uses a fast, lock-free sequence generator to ensure deterministic mathematical distribution.
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

// String provides a stable name for stats reporting.
func (s *AdaptiveStrategy) String() string {
	return "Adaptive"
}
