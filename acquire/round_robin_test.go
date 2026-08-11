package acquire_test

import (
	"sync"
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/acquire"
)

// mockView implements acquire.ShardView for testing.
type mockView struct {
	count int
}

func (m mockView) ShardCount() int {
	return m.count
}

func (m mockView) ActiveCount(shard int) int {
	return 0
}

func TestRoundRobin_Cycle(t *testing.T) {
	strategy := acquire.NewRoundRobin()
	view := mockView{count: 3}

	// Should cycle: 0, 1, 2, 0, 1, 2...
	expected := []int{0, 1, 2, 0, 1, 2, 0}

	for i, exp := range expected {
		got := strategy.Select(view)
		if got != exp {
			t.Errorf("iteration %d: expected shard %d, got %d", i, exp, got)
		}
	}
}

func TestRoundRobin_ZeroOrNegativeShards(t *testing.T) {
	strategy := acquire.NewRoundRobin()

	// Should not panic (division by zero)
	viewZero := mockView{count: 0}
	if got := strategy.Select(viewZero); got != 0 {
		t.Errorf("expected 0 for ShardCount=0, got %d", got)
	}

	viewNegative := mockView{count: -5}
	if got := strategy.Select(viewNegative); got != 0 {
		t.Errorf("expected 0 for negative ShardCount, got %d", got)
	}
}

func TestRoundRobin_Concurrent(t *testing.T) {
	strategy := acquire.NewRoundRobin()
	view := mockView{count: 4}

	var wg sync.WaitGroup
	numWorkers := 100
	numOps := 1000

	results := make(chan int, numWorkers*numOps)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				results <- strategy.Select(view)
			}
		}()
	}

	wg.Wait()
	close(results)

	// Verify perfect distribution
	counts := make(map[int]int)
	for shardIdx := range results {
		counts[shardIdx]++
	}

	expectedPerShard := (numWorkers * numOps) / 4
	for i := 0; i < 4; i++ {
		if counts[i] != expectedPerShard {
			t.Errorf("expected %d hits for shard %d, got %d", expectedPerShard, i, counts[i])
		}
	}
}
