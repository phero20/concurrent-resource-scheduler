package placement_test

import (
	"hash/fnv"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/placement"
)

func TestConsistentHashRing_Deterministic(t *testing.T) {
	ring := placement.NewConsistentHashRing(4)

	h := fnv.New64a()
	h.Write([]byte("test-key-1"))
	hash1 := h.Sum64()

	shard1 := ring.GetShard(hash1)
	shard2 := ring.GetShard(hash1)

	if shard1 != shard2 {
		t.Fatalf("Expected deterministic shard selection, got %d and %d", shard1, shard2)
	}
}

func TestConsistentHashRing_Distribution(t *testing.T) {
	numShards := 4
	ring := placement.NewConsistentHashRing(numShards)

	counts := make(map[int]int)
	numKeys := 100000

	for i := 0; i < numKeys; i++ {
		h := fnv.New64a()
		h.Write([]byte("affinity-key-" + strconv.Itoa(i)))
		shard := ring.GetShard(h.Sum64())
		counts[shard]++
	}

	expected := float64(numKeys) / float64(numShards)
	tolerance := expected * 0.50

	for shard, count := range counts {
		diff := math.Abs(float64(count) - expected)
		if diff > tolerance {
			t.Errorf("Shard %d received %d keys, expected ~%v (tolerance: %v)", shard, count, expected, tolerance)
		}
	}
}

func TestConsistentHashRing_MinimalReshuffling(t *testing.T) {
	shardsBefore := 4
	shardsAfter := 5

	ring4 := placement.NewConsistentHashRing(shardsBefore)
	ring5 := placement.NewConsistentHashRing(shardsAfter)

	numKeys := 100000
	remapped := 0

	for i := 0; i < numKeys; i++ {
		h := fnv.New64a()
		h.Write([]byte("affinity-key-" + strconv.Itoa(i)))
		hashVal := h.Sum64()

		s4 := ring4.GetShard(hashVal)
		s5 := ring5.GetShard(hashVal)

		if s4 != s5 {
			remapped++
		}
	}

	expectedRemapped := float64(numKeys) / float64(shardsAfter)
	maxAllowed := expectedRemapped * 1.25

	if float64(remapped) > maxAllowed {
		t.Fatalf("Reshuffling too high! Remapped %d keys out of %d (expected ~%v)", remapped, numKeys, expectedRemapped)
	}
}

func TestConsistentHashRing_ConcurrentReads(t *testing.T) {
	ring := placement.NewConsistentHashRing(4)

	var wg sync.WaitGroup
	numWorkers := 100
	numOps := 1000

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				_ = ring.GetShard(uint64(j))
			}
		}()
	}

	wg.Wait()
}

func BenchmarkConsistentHashRing_GetShard(b *testing.B) {
	ring := placement.NewConsistentHashRing(4)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ring.GetShard(uint64(i))
	}
}
