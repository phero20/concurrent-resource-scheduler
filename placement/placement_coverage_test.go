package placement_test

import (
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/placement"
)

type mockEmptyShardView struct{}

func (m mockEmptyShardView) ShardCount() int           { return 0 }
func (m mockEmptyShardView) ActiveCount(shard int) int { return 0 }

func TestPlacement_StringMethods(t *testing.T) {
	rr, ok := placement.NewRoundRobin().(interface{ String() string })
	if !ok || rr.String() != "RoundRobin" {
		t.Errorf("Expected RoundRobin String method")
	}

	ad := placement.NewAdaptiveStrategy()
	if ad.String() != "Adaptive" {
		t.Errorf("Expected Adaptive String method")
	}

	wt := placement.NewWeightedStrategy([]uint{1, 1})
	if wt.String() != "Weighted" {
		t.Errorf("Expected Weighted String method")
	}
}

func TestWeightedStrategy_ZeroShards(t *testing.T) {
	wt := placement.NewWeightedStrategy([]uint{1, 1})

	shard := wt.Select(mockEmptyShardView{})
	if shard != 0 {
		t.Errorf("Expected 0 for empty shard view, got %d", shard)
	}
}
