package acquire

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// virtualNodesPerShard determines the statistical uniformity of the hash ring.
// Higher values improve uniformity at the cost of initialization memory.
// 500 provides a well-balanced distribution with roughly ~4.5% standard deviation.
const virtualNodesPerShard = 500

// ConsistentHashRing maps arbitrary uint64 hashes to stable Heap Shard indices
// using virtual nodes for uniform distribution.
//
// ConsistentHashRing is used internally by [scheduler.Scheduler.AcquireByAffinity]
// to implement sticky-session routing. It is not an [AcquireStrategy] and is
// not configurable via [config.Config.AcquireStrategy].
//
// Virtual nodes improve hash distribution and minimize remapping when the
// shard count hypothetically changes. The ring topology is built once at
// scheduler construction time and is immutable thereafter.
//
// # Concurrency
//
// ConsistentHashRing is safe for concurrent use by multiple goroutines after
// construction. GetShard is allocation-free and does not acquire any lock.
type ConsistentHashRing struct {
	hashes []uint64
	shards []int
}

// NewConsistentHashRing constructs a ConsistentHashRing for the given number
// of Heap Shards.
//
// Each shard is represented by [virtualNodesPerShard] (500) virtual nodes on
// the ring, providing roughly 4.5% standard deviation in distribution. If
// shardCount is zero or negative, it is treated as 1.
//
// Complexity: O(V log V) where V = shardCount × virtualNodesPerShard.
func NewConsistentHashRing(shardCount int) *ConsistentHashRing {
	if shardCount <= 0 {
		shardCount = 1
	}

	totalNodes := shardCount * virtualNodesPerShard

	type vnode struct {
		hash  uint64
		shard int
	}
	vnodes := make([]vnode, 0, totalNodes)

	for i := 0; i < shardCount; i++ {
		for j := 0; j < virtualNodesPerShard; j++ {
			h := fnv.New64a()
			// Generate deterministic hash for the virtual node
			_, _ = h.Write([]byte("shard-" + strconv.Itoa(i) + "-vnode-" + strconv.Itoa(j)))

			vnodes = append(vnodes, vnode{
				hash:  h.Sum64(),
				shard: i,
			})
		}
	}

	// Sort virtual nodes by hash value
	sort.Slice(vnodes, func(i, j int) bool {
		return vnodes[i].hash < vnodes[j].hash
	})

	hashes := make([]uint64, totalNodes)
	shards := make([]int, totalNodes)

	for i, v := range vnodes {
		hashes[i] = v.hash
		shards[i] = v.shard
	}

	return &ConsistentHashRing{
		hashes: hashes,
		shards: shards,
	}
}

// GetShard maps a 64-bit hash to the nearest Heap Shard on the ring using
// binary search. If the hash falls past the last virtual node, it wraps around
// to the first node (ring semantics).
//
// Complexity: O(log V) where V is the total number of virtual nodes.
func (r *ConsistentHashRing) GetShard(hash uint64) int {
	if len(r.hashes) == 0 {
		return 0
	}

	// Manual binary search to guarantee zero allocations in the hot path
	i, j := 0, len(r.hashes)
	for i < j {
		h := int(uint(i+j) >> 1)
		if r.hashes[h] < hash {
			i = h + 1
		} else {
			j = h
		}
	}

	if i == len(r.hashes) {
		i = 0
	}

	return r.shards[i]
}
