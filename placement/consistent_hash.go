package placement

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// virtualNodesPerShard determines the statistical uniformity of the hash ring.
// Higher values improve uniformity at the cost of initialization memory.
// 500 provides a well-balanced distribution with roughly ~4.5% standard deviation.
const virtualNodesPerShard = 500

// ConsistentHashRing maps arbitrary uint64 hashes to stable Heap Shards.
//
// Behavior:
// Utilizes virtual nodes to ensure even distribution and minimizes migration
// if shard counts hypothetically change.
//
// Concurrency Guarantees:
// Thread-safe. The ring topology is immutable after creation.
type ConsistentHashRing struct {
	hashes []uint64
	shards []int
}

// NewConsistentHashRing constructs a hash ring tailored to the given shard count.
//
// Complexity: O(V log V) where V is virtual nodes (fixed ratio to shard count).
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

// GetShard performs a binary search to find the closest virtual node on the ring.
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
