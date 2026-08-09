package main

import (
	"fmt"
	"log"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)

type Worker struct {
	ID       string
	Priority int
}

func main() {
	compare := func(a, b *Worker) int {
		if a.Priority < b.Priority {
			return -1
		}
		if a.Priority > b.Priority {
			return 1
		}
		return 0
	}
	keyFunc := func(w *Worker) string { return w.ID }

	cfg := config.Config[*Worker, string]{
		HeapCount:  4, // Distribute across 4 shards
		Comparator: compare,
		KeyFunc:    keyFunc,
	}

	sched, err := scheduler.New(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize scheduler: %v", err)
	}
	defer sched.Shutdown()

	// 1. Prepare a large batch of resources.
	batch := make([]*Worker, 100)
	for i := 0; i < 100; i++ {
		batch[i] = &Worker{
			ID:       fmt.Sprintf("worker-%d", i),
			Priority: i,
		}
	}

	// 2. Insert all resources atomically.
	// BatchAdd validates the entire slice (no duplicates, no nils) before
	// modifying any internal state.
	if err := sched.BatchAdd(batch); err != nil {
		log.Fatalf("Failed to add batch: %v", err)
	}

	fmt.Printf("Successfully added %d resources.\n", sched.Len())
}
