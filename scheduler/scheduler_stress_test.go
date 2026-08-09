package scheduler_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/config"
	"github.com/phero20/concurrent-resource-scheduler/scheduler"
)

func TestScheduler_MixedConcurrentStress(t *testing.T) {
	cfg := validConfig(4)
	cfg.AcquirePolicy = config.Exclusive
	s, _ := scheduler.New(cfg)

	numWorkers := 100
	numOps := 100
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				id := "w" + strconv.Itoa(workerID) + "-op" + strconv.Itoa(j)

				// Pick an operation pseudorandomly based on index
				op := (workerID + j) % 8
				switch op {
				case 0:
					_ = s.Add(&Resource{ID: id, Priority: j})
				case 1:
					_, _ = s.Acquire()
				case 2:
					_ = s.Release(id)
				case 3:
					_ = s.Update(&Resource{ID: id, Priority: j + 10})
				case 4:
					_ = s.Remove(id)
				case 5:
					_ = s.Exclude(id)
				case 6:
					_ = s.Include(id)
				case 7:
					_, _ = s.Get(id)
				}
			}
		}(i)
	}

	wg.Wait()
}
