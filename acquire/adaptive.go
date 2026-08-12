package acquire

import (
	"sync/atomic"
)

// AdaptiveStrategy selects Heap Shards using the power-of-two random choices
// algorithm: it picks two shards at random and selects the one with fewer
// active resources.
//
// This approach probabilistically favors less-loaded shards without scanning
// all shards on every call (O(1) shard comparison, O(H) per call due to
// reading active counts). It outperforms [NewRoundRobin] when resource
// acquisition times vary significantly across shards and when active counts
// serve as a meaningful proxy for load.
//
// AdaptiveStrategy is well-suited for dynamic workloads where some resources
// become temporarily unavailable (due to Exclusive acquisition, Exclude, or
// cooldown) and others are idle.
//
// For static, homogeneous workloads, [NewRoundRobin] has lower overhead.
// For heterogeneous capacity, [NewWeightedStrategy] is more predictable.
//
// # Concurrency
//
// AdaptiveStrategy is safe for concurrent use by multiple goroutines. It uses
// a single atomic increment for its PRNG seed and makes only non-blocking
// atomic reads of shard active counts.
type AdaptiveStrategy struct {
	counter atomic.Uint64
}

// NewAdaptiveStrategy creates a load-aware acquire strategy that uses the
// power-of-two random choices algorithm to favor Heap Shards with fewer active
// resources.
//
// Provide the returned value as [config.Config.AcquireStrategy] during
// scheduler construction.
//
// The returned strategy is safe for concurrent use by multiple goroutines.
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
