package scheduler_test

import (
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/placement"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

type activeCountTestStrategy struct {
	t *testing.T
}

func (s *activeCountTestStrategy) Select(view placement.ShardView) int {
	// Test valid shard
	if view.ActiveCount(0) != 1 {
		s.t.Errorf("Expected 1 active resource in shard 0, got %d", view.ActiveCount(0))
	}
	
	// Test invalid shards
	if view.ActiveCount(-1) != 0 {
		s.t.Errorf("Expected 0 active for negative shard, got %d", view.ActiveCount(-1))
	}
	
	if view.ActiveCount(view.ShardCount()) != 0 {
		s.t.Errorf("Expected 0 active for out of bounds shard, got %d", view.ActiveCount(view.ShardCount()))
	}
	
	return 0
}

func TestShardView_ActiveCount(t *testing.T) {
	cfg := validConfig(1)
	cfg.AcquireStrategy = &activeCountTestStrategy{t: t}
	
	sched, _ := scheduler.New(cfg)
	sched.Add(&Resource{ID: "r1", Priority: 1})
	
	// Trigger strategy
	_, _ = sched.Acquire()
}
