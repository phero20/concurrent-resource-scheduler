package placement_test

import (
	"sync"
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/placement"
)

// adaptiveMockView implements placement.ShardView for testing adaptive load balancing.
type adaptiveMockView struct {
	count        int
	activeCounts map[int]int
}

func (m adaptiveMockView) ShardCount() int {
	return m.count
}

func (m adaptiveMockView) ActiveCount(shard int) int {
	if m.activeCounts != nil {
		return m.activeCounts[shard]
	}
	return 0
}

func TestAdaptiveStrategy_Distribution(t *testing.T) {
	strategy := placement.NewAdaptiveStrategy()

	// Shard 0 is heavily loaded, Shard 1 is medium, Shard 2 is lightly loaded, Shard 3 is empty
	view := adaptiveMockView{
		count: 4,
		activeCounts: map[int]int{
			0: 1000,
			1: 500,
			2: 10,
			3: 0,
		},
	}

	counts := make(map[int]int)
	numOps := 100000

	for i := 0; i < numOps; i++ {
		shard := strategy.Select(view)
		counts[shard]++
	}

	// Because it's Power of Two Choices, the least loaded shards (2 and 3) should receive the vast majority of traffic.
	// Shard 0 (heaviest) should only be selected if BOTH choices land on Shard 0 (1/16 chance).
	// Shard 1 (medium) should only be selected if choices are (1,1) or (1,0) or (0,1).
	
	expectedShard0 := float64(numOps) * (1.0 / 16.0)
	
	if float64(counts[0]) > expectedShard0*1.5 {
		t.Errorf("Shard 0 received too much traffic: %d (expected ~%v)", counts[0], expectedShard0)
	}

	if counts[3] < counts[2] || counts[2] < counts[1] || counts[1] < counts[0] {
		t.Errorf("Traffic was not routed inversely to load. Counts: %v", counts)
	}
}

func TestAdaptiveStrategy_ZeroOrOneShard(t *testing.T) {
	strategy := placement.NewAdaptiveStrategy()

	shard0 := strategy.Select(adaptiveMockView{count: 0})
	if shard0 != 0 {
		t.Errorf("Expected 0 for empty view, got %d", shard0)
	}

	shard1 := strategy.Select(adaptiveMockView{count: 1})
	if shard1 != 0 {
		t.Errorf("Expected 0 for single shard view, got %d", shard1)
	}
}

func TestAdaptiveStrategy_ConcurrentSelect(t *testing.T) {
	strategy := placement.NewAdaptiveStrategy()
	view := adaptiveMockView{count: 4, activeCounts: map[int]int{0: 10, 1: 5, 2: 2, 3: 0}}

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

func BenchmarkAdaptiveStrategy_Select(b *testing.B) {
	strategy := placement.NewAdaptiveStrategy()
	var view placement.ShardView = adaptiveMockView{count: 4, activeCounts: map[int]int{0: 10, 1: 5, 2: 2, 3: 0}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = strategy.Select(view)
	}
}
