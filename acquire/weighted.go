package acquire

import (
	"sync/atomic"
)

// mix applies a fast, allocation-free splitmix64 avalanche to an integer.
func mix(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// WeightedStrategy distributes traffic proportionally based on static capacity
// weights provided at construction time.
//
// Each weight corresponds to a Heap Shard by position. Higher-weight shards
// receive a proportionally larger fraction of incoming acquisition requests.
// The distribution is driven by a lock-free splitmix64 avalanche PRNG for
// statistical uniformity.
//
// WeightedStrategy is well-suited for heterogeneous resource pools, such as:
//
//   - GPU workers with different VRAM capacities.
//   - API providers with different rate-limit quotas.
//   - Backend instances with different throughput characteristics.
//
// Weights are static: they cannot be changed after construction. If the
// resource pool changes shape, create a new WeightedStrategy.
//
// # Fallback Behavior
//
// WeightedStrategy falls back to uniform distribution (equivalent to
// [NewRoundRobin]) when any of the following conditions hold:
//
//   - The weights slice is empty.
//   - The total of all weights is zero (all-zero weights).
//   - The number of weights does not equal ShardView.ShardCount() at
//     selection time (shard count mismatch).
//
// # Concurrency
//
// WeightedStrategy is safe for concurrent use by multiple goroutines.
// The internal state advances with a single atomic increment per Select call.
type WeightedStrategy struct {
	cumulativeWeights []uint64
	totalWeight       uint64
	counter           atomic.Uint64
}

// NewWeightedStrategy creates a capacity-aware acquire strategy.
//
// weights is a slice of non-negative integers, one per Heap Shard in the
// order they were created. A weight of 0 means the shard receives no traffic
// when the slice has non-zero weights elsewhere. All weights summing to zero
// triggers the uniform-distribution fallback.
//
// The length of weights must equal the scheduler's HeapCount at acquire time;
// a mismatch triggers the uniform-distribution fallback. For a stable
// configuration, supply a slice with exactly [config.Config.HeapCount] elements.
//
// The returned strategy is safe for concurrent use by multiple goroutines.
func NewWeightedStrategy(weights []uint) *WeightedStrategy {
	cumulative := make([]uint64, len(weights))
	var total uint64
	for i, w := range weights {
		total += uint64(w)
		cumulative[i] = total
	}

	return &WeightedStrategy{
		cumulativeWeights: cumulative,
		totalWeight:       total,
	}
}

// Select performs an O(log W) binary search over the accumulated weight distribution.
//
// Complexity: O(log W) where W is the number of provided weights.
func (s *WeightedStrategy) Select(view ShardView) int {
	shardCount := view.ShardCount()
	if shardCount == 0 {
		return 0
	}

	// Increment the sequence counter.
	c := s.counter.Add(1)

	// Fallback to uniform distribution if no valid weights are configured,
	// or if the shard count does not match the configured weights length.
	if s.totalWeight == 0 || len(s.cumulativeWeights) != shardCount {
		return int(c % uint64(shardCount))
	}

	// Apply splitmix64 for perfectly uniform distribution over the sequence.
	hashVal := mix(c)
	target := hashVal % s.totalWeight

	// Unrolled binary search over the cumulative weights to find the target shard.
	i, j := 0, shardCount
	for i < j {
		h := int(uint(i+j) >> 1)
		if s.cumulativeWeights[h] <= target {
			i = h + 1
		} else {
			j = h
		}
	}

	if i >= shardCount {
		return 0
	}

	return i
}

// String identifies the strategy.
//
// Complexity: O(1).
func (s *WeightedStrategy) String() string {
	return "Weighted"
}
