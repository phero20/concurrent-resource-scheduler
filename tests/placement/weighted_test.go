package placement_test

import (
	"math"
	"sync"
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/placement"
)

func TestWeightedStrategy_Distribution(t *testing.T) {
	weights := []uint{10, 20, 70} // 10%, 20%, 70%
	strategy := placement.NewWeightedStrategy(weights)
	view := mockView{count: 3} // mockView from round_robin_test.go

	counts := make(map[int]int)
	numOps := 1000000

	for i := 0; i < numOps; i++ {
		shard := strategy.Select(view)
		counts[shard]++
	}

	totalWeight := 100.0
	tolerance := 0.01 // 1% tolerance over 1,000,000 iterations is plenty for PRNGs

	for i, w := range weights {
		expectedPct := float64(w) / totalWeight
		actualPct := float64(counts[i]) / float64(numOps)

		if math.Abs(expectedPct-actualPct) > tolerance {
			t.Errorf("Shard %d received %v%%, expected ~%v%%", i, actualPct*100, expectedPct*100)
		}
	}
}

func TestWeightedStrategy_ZeroWeight(t *testing.T) {
	// If a weight is zero, it should never be selected
	weights := []uint{0, 100, 0}
	strategy := placement.NewWeightedStrategy(weights)
	view := mockView{count: 3}

	for i := 0; i < 10000; i++ {
		shard := strategy.Select(view)
		if shard != 1 {
			t.Fatalf("Expected shard 1, got %d", shard)
		}
	}
}

func TestWeightedStrategy_AllZerosFallback(t *testing.T) {
	// If all weights are zero, it should fallback to uniform round-robin
	weights := []uint{0, 0, 0}
	strategy := placement.NewWeightedStrategy(weights)
	view := mockView{count: 3}

	counts := make(map[int]int)
	for i := 0; i < 3000; i++ {
		shard := strategy.Select(view)
		counts[shard]++
	}

	// Should be perfectly uniform (1000 each) because the fallback is a simple counter modulo
	for i := 0; i < 3; i++ {
		if counts[i] != 1000 {
			t.Errorf("Expected 1000 for shard %d, got %d", i, counts[i])
		}
	}
}

func TestWeightedStrategy_MismatchedShardCountFallback(t *testing.T) {
	// If the weights length doesn't match the shard count, fallback to uniform
	weights := []uint{10, 90} // 2 weights
	strategy := placement.NewWeightedStrategy(weights)
	view := mockView{count: 4} // 4 shards in view

	counts := make(map[int]int)
	for i := 0; i < 4000; i++ {
		shard := strategy.Select(view)
		counts[shard]++
	}

	for i := 0; i < 4; i++ {
		if counts[i] != 1000 {
			t.Errorf("Expected fallback uniform distribution, got %d for shard %d", counts[i], i)
		}
	}
}

func TestWeightedStrategy_ConcurrentSelect(t *testing.T) {
	weights := []uint{10, 20, 30, 40}
	strategy := placement.NewWeightedStrategy(weights)
	view := mockView{count: 4}

	var wg sync.WaitGroup
	numWorkers := 100
	numOps := 10000

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				_ = strategy.Select(view)
			}
		}()
	}

	wg.Wait()
}

func BenchmarkWeightedStrategy_Select(b *testing.B) {
	weights := []uint{10, 20, 30, 40}
	strategy := placement.NewWeightedStrategy(weights)
	view := mockView{count: 4}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = strategy.Select(view)
	}
}
