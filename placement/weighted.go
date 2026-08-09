package placement

import (
	"sync/atomic"
)

// mix applies a fast, allocation-free splitmix64 avalanche to an integer.
func mix(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// WeightedStrategy distributes traffic proportionally based on static capacities.
//
// Behavior:
// Higher weight shards receive more traffic. It utilizes an internal splitmix64
// avalanche RNG per selection to maintain statistical distribution.
//
// Concurrency Guarantees:
// Thread-safe. The internal state advances atomically.
type WeightedStrategy struct {
	cumulativeWeights []uint64
	totalWeight       uint64
	counter           atomic.Uint64
}

// NewWeightedStrategy creates a capacity-aware placement strategy.
//
// Behavior:
// Reverts to RoundRobin if weights are empty or uniform.
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
