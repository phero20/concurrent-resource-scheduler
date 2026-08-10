package scheduler_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/placement"
	"github.com/phero20/concurrent-resource-scheduler/scheduler"
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

func TestScheduler_ConcurrentAdaptiveActiveCountRace(t *testing.T) {
	cfg := validConfig(4)
	cfg.AcquireStrategy = placement.NewAdaptiveStrategy()

	sched, err := scheduler.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	var mutatorWg sync.WaitGroup
	var readerWg sync.WaitGroup
	done := make(chan struct{})

	workers := 4
	opsPerWorker := 500

	for i := 0; i < workers; i++ {
		mutatorWg.Add(1)
		go func(workerID int) {
			defer mutatorWg.Done()
			for j := 0; j < opsPerWorker; j++ {
				res := &Resource{ID: fmt.Sprintf("r-%d-%d", workerID, j), Priority: j}
				_ = sched.Add(res)
				acq, _ := sched.Acquire()
				if acq != nil {
					_ = sched.Release(acq.ID)
				}
			}
		}(i)
	}

	for i := 0; i < 4; i++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for {
				select {
				case <-done:
					return
				default:
					st := sched.Stats()
					if st.ActiveResources < 0 {
						t.Errorf("Invalid ActiveResources: %d", st.ActiveResources)
					}
				}
			}
		}()
	}

	mutatorWg.Wait()
	close(done)
	readerWg.Wait()
}
