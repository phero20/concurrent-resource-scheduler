package acquire_test

import (
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/acquire"
)

type mockEmptyShardView struct{}

func (m mockEmptyShardView) ShardCount() int           { return 0 }
func (m mockEmptyShardView) ActiveCount(shard int) int { return 0 }

func TestAcquire_StringMethods(t *testing.T) {
	rr, ok := acquire.NewRoundRobin().(interface{ String() string })
	if !ok || rr.String() != "RoundRobin" {
		t.Errorf("Expected RoundRobin String method")
	}

	ad := acquire.NewAdaptiveStrategy()
	if ad.String() != "Adaptive" {
		t.Errorf("Expected Adaptive String method")
	}

	wt := acquire.NewWeightedStrategy([]uint{1, 1})
	if wt.String() != "Weighted" {
		t.Errorf("Expected Weighted String method")
	}
}

func TestWeightedStrategy_ZeroShards(t *testing.T) {
	wt := acquire.NewWeightedStrategy([]uint{1, 1})

	shard := wt.Select(mockEmptyShardView{})
	if shard != 0 {
		t.Errorf("Expected 0 for empty shard view, got %d", shard)
	}
}
