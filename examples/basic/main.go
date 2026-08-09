package main

import (
	"fmt"
	"log"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

// Resource represents a mock application resource (e.g., a worker or proxy).
type Resource struct {
	ID       string
	Priority int
}

func main() {
	// 1. Define priority logic: smaller Priority value = higher priority.
	compare := func(a, b *Resource) int {
		if a.Priority < b.Priority {
			return -1
		}
		if a.Priority > b.Priority {
			return 1
		}
		return 0
	}

	// 2. Define identity extractor.
	keyFunc := func(r *Resource) string {
		return r.ID
	}

	// 3. Configure the scheduler. We use 1 heap for simplicity.
	cfg := config.Config[*Resource, string]{
		HeapCount:  1,
		Comparator: compare,
		KeyFunc:    keyFunc,
	}

	// 4. Create the scheduler.
	sched, err := scheduler.New(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize scheduler: %v", err)
	}
	defer sched.Shutdown()

	// 5. Add some resources.
	sched.Add(&Resource{ID: "res-1", Priority: 50})
	sched.Add(&Resource{ID: "res-2", Priority: 10}) // Best priority

	// 6. Acquire the highest priority resource.
	res, err := sched.Acquire()
	if err != nil {
		log.Fatalf("Failed to acquire: %v", err)
	}

	// Output: Acquired res-2 (Priority: 10)
	fmt.Printf("Acquired %s (Priority: %d)\n", res.ID, res.Priority)
}
